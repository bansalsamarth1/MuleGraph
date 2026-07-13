package com.mulegraph.fraud.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "mulegraph.rules.shared-device")
public class SharedDeviceRuleProperties {
    private int windowSeconds = 60;
    private int graceSeconds = 15;
    private int minDistinctSources = 3;
    private int minTransactionCount = 0;
    private long minTotalAmountMinor = 0;

    public int getWindowSeconds() {
        return windowSeconds;
    }

    public void setWindowSeconds(int windowSeconds) {
        this.windowSeconds = windowSeconds;
    }

    public int getGraceSeconds() {
        return graceSeconds;
    }

    public void setGraceSeconds(int graceSeconds) {
        this.graceSeconds = graceSeconds;
    }

    public int getMinDistinctSources() {
        return minDistinctSources;
    }

    public void setMinDistinctSources(int minDistinctSources) {
        this.minDistinctSources = minDistinctSources;
    }

    public int getMinTransactionCount() {
        return minTransactionCount;
    }

    public void setMinTransactionCount(int minTransactionCount) {
        this.minTransactionCount = minTransactionCount;
    }

    public long getMinTotalAmountMinor() {
        return minTotalAmountMinor;
    }

    public void setMinTotalAmountMinor(long minTotalAmountMinor) {
        this.minTotalAmountMinor = minTotalAmountMinor;
    }
}
