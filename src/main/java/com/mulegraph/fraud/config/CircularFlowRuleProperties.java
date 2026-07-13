package com.mulegraph.fraud.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "mulegraph.rules.circular-flow")
public class CircularFlowRuleProperties {

    private int maxDepth = 4;
    private long windowSeconds = 86400;
    private int amountTolerancePercent = 10;

    public int getMaxDepth() {
        return maxDepth;
    }

    public void setMaxDepth(int maxDepth) {
        this.maxDepth = maxDepth;
    }

    public long getWindowSeconds() {
        return windowSeconds;
    }

    public void setWindowSeconds(long windowSeconds) {
        this.windowSeconds = windowSeconds;
    }

    public int getAmountTolerancePercent() {
        return amountTolerancePercent;
    }

    public void setAmountTolerancePercent(int amountTolerancePercent) {
        this.amountTolerancePercent = amountTolerancePercent;
    }
}
