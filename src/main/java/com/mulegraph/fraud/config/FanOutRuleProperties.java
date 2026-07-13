package com.mulegraph.fraud.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "mulegraph.rules.fan-out")
public class FanOutRuleProperties {
    private int windowSeconds = 60;
    private int graceSeconds = 15;
    private int minDistinctDestinations = 5;

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
}
