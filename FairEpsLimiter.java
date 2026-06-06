package com.dptech.sensor.limit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * 日志采集公平调度限流器 (V2 水位优先级版)。
 * <p>
 * 核心设计（解决原滑动窗口的时间差饿死与边界突发问题）：
 * 1. 令牌桶硬限流：使用无锁令牌桶替代固定窗口，平滑流量。容量400，生成率4000/s，彻底解决固定窗口临界点突发8000的问题。
 * 2. 水位优先级机制（核心）：根据 IP 历史发包速率与公平份额的比例，动态划分高/低优先级。
 * 3. 额度预留防饿死：令牌桶为高优先级（未超额）IP 预留 50% 的 Token。
 *    - 无竞争时：低优 IP 可借用额度，消耗令牌直到触及预留水位，从而持续以 4000/s 独占全局额度。
 *    - 发生竞争时：高优 IP 可无视预留水位直接获取底层令牌，低优 IP 被瞬间拦截，完美解决“提前发包导致其他IP饿死”的问题。
 * 4. 近似滑动窗口：采用 Cloudflare 双窗口加权算法估算 IP 速率，O(1) 时间与内存复杂度，彻底消除原先 O(N) 遍历造成的 CPU 瓶颈。
 *
 * @author liming
 * @date 2026-05-14
 */
@Component
public class FairEpsLimiter {
    private static final Logger LOGGER = LoggerFactory.getLogger(FairEpsLimiter.class);

    /** 全局每秒最大事件数（硬上限令牌生成率） */
    @org.springframework.beans.factory.annotation.Value("${syslog.limiter.globalRate:4000}")
    private int globalRate;

    /** 令牌桶最大容量（允许的最大瞬间突发量，防止单IP一瞬间抽干全局份额） */
    @org.springframework.beans.factory.annotation.Value("${syslog.limiter.burst:1000}")
    private int burstCapacity;

    /** 为高优先级（未达公平份额的IP）预留的令牌数 */
    @org.springframework.beans.factory.annotation.Value("${syslog.limiter.reserve:500}")
    private int highPriorityReserve;

    private final AtomicLong globalTokens = new AtomicLong(1000);
    private final AtomicLong lastRefillNanos = new AtomicLong(System.nanoTime());
    private final AtomicBoolean discardWarnLogged = new AtomicBoolean(false);

    // 统计指标
    private final LongAdder totalReceived = new LongAdder();
    private final LongAdder totalDropped = new LongAdder();

    /**
     * 1秒时间窗口数据结构
     */
    private static class Window {
        final long startSec;
        final ConcurrentHashMap<String, LongAdder> counts = new ConcurrentHashMap<>();
        final AtomicInteger activeCount = new AtomicInteger(0);

        Window(long startSec) {
            this.startSec = startSec;
        }
    }

    private volatile Window prevWindow = new Window(System.currentTimeMillis() / 1000 - 1);
    private volatile Window currWindow = new Window(System.currentTimeMillis() / 1000);

    public boolean tryAcquire(String ip) {
        totalReceived.increment();
        if (ip == null || ip.isEmpty()) {
            ip = "unknown";
        }

        long now = System.currentTimeMillis();
        long currentSec = now / 1000;
        
        // 1. 滚动时间窗口
        slideWindow(currentSec);

        // 2. 统计当前 IP 的请求量
        Window curr = currWindow;
        Window prev = prevWindow;

        curr.counts.computeIfAbsent(ip, k -> {
            curr.activeCount.incrementAndGet();
            return new LongAdder();
        }).increment();

        // 3. 计算 IP 近期平滑速率 (Cloudflare 双窗口加权估算法)
        // 优化：仅在必要时才调用 sum()，减少 CPU 开销
        LongAdder currAdder = curr.counts.get(ip);
        long currCount = currAdder != null ? currAdder.sum() : 0;
        LongAdder prevAdder = prev.counts.get(ip);
        long prevCount = prevAdder == null ? 0 : prevAdder.sum();
        
        // 当前秒已过去的比例
        double weight = 1.0 - ((now % 1000) / 1000.0);
        long estimatedRate = (long) (prevCount * weight + currCount);

        // 4. 计算动态公平份额
        int activeIPs = Math.max(1, Math.max(curr.activeCount.get(), prev.activeCount.get()));
        long fairShare = globalRate / activeIPs;

        // 5. 判断优先级：未超公平份额的为高优，超出的为低优（处于借用态）
        boolean highPriority = estimatedRate <= fairShare;

        // 6. 尝试从全局无锁令牌桶获取许可
        boolean acquired = tryAcquireGlobalToken(highPriority);
        
        if (!acquired) {
            totalDropped.increment();
            logDiscardOnce(ip, activeIPs, estimatedRate, fairShare);
        }
        
        return acquired;
    }

    private void slideWindow(long currentSec) {
        Window curr = currWindow;
        if (currentSec > curr.startSec) {
            synchronized (this) {
                if (currentSec > currWindow.startSec) {
                    if (currentSec == currWindow.startSec + 1) {
                        prevWindow = currWindow;
                    } else {
                        // 期间没有流量，跨越了多个秒
                        prevWindow = new Window(currentSec - 1);
                    }
                    currWindow = new Window(currentSec);
                    
                    // 新秒开始，重置日志打印标志，允许每秒打印一次警告
                    discardWarnLogged.set(false);
                    
                    // 打印每秒统计数据
                    if (LOGGER.isInfoEnabled()) {
                        long dropped = totalDropped.sumThenReset();
                        long received = totalReceived.sumThenReset();
                        if (received > 0 || dropped > 0) {
                            LOGGER.info("限流统计: 接收={}, 丢弃={}, 活跃IP={}, 全局限额={}/s", 
                                received, dropped, activeIPs(), globalRate);
                        }
                    }
                }
            }
        }
    }

    private int activeIPs() {
        return Math.max(1, Math.max(currWindow.activeCount.get(), prevWindow.activeCount.get()));
    }

    private boolean tryAcquireGlobalToken(boolean highPriority) {
        long now = System.nanoTime();
        long lastRefill = lastRefillNanos.get();
        long deltaNanos = now - lastRefill;
        long nanosPerToken = 1_000_000_000L / globalRate;

        // 1. 惰性补充令牌
        if (deltaNanos >= nanosPerToken) {
            long tokensToAdd = deltaNanos / nanosPerToken;
            long nanosToAdvance = tokensToAdd * nanosPerToken;
            
            if (lastRefillNanos.compareAndSet(lastRefill, lastRefill + nanosToAdvance)) {
                long currentTokens;
                long newTokens;
                do {
                    currentTokens = globalTokens.get();
                    newTokens = Math.min(burstCapacity, currentTokens + tokensToAdd);
                } while (!globalTokens.compareAndSet(currentTokens, newTokens));
            }
        }

        // 2. CAS 扣减令牌
        long reserve = highPriority ? 0 : highPriorityReserve;
        
        // 优化 CAS 循环，限制重试次数，避免极端情况下的 CPU 飙升
        for (int i = 0; i < 10; i++) {
            long currentTokens = globalTokens.get();
            if (currentTokens - 1 < reserve) {
                return false;
            }
            if (globalTokens.compareAndSet(currentTokens, currentTokens - 1)) {
                return true;
            }
        }
        return false;
    }

    private void logDiscardOnce(String ip, int activeIPs, long estimatedRate, long fairShare) {
        if (LOGGER.isWarnEnabled() && discardWarnLogged.compareAndSet(false, true)) {
            LOGGER.warn("日志采集达到公平调度限流阈值, 全局限额={}/s, 活跃IP数={}, 触发IP={} (其预估速率={}, 公平份额={})",
                    globalRate, activeIPs, ip, estimatedRate, fairShare);
        }
    }

    public int getGlobalLimit() {
        return globalRate;
    }
}
