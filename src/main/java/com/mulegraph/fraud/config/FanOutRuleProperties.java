package com.mulegraph.fraud.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "mulegraph.rules.fan-out")
public class FanOutRuleProperties {
    private int windowSeconds = 60;
    private int graceSeconds = 15;
    private int minDistinctDestinations = 5;
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

    public int getMinDistinctDestinations() {
        return minDistinctDestinations;
    }

    public void setMinDistinctDestinations(int minDistinctDestinations) {
        this.minDistinctDestinations = minDistinctDestinations;
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
