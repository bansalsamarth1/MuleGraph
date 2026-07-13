package com.mulegraph.fraud.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "mulegraph.rules.shared-device")
public class SharedDeviceRuleProperties {
    private int windowSeconds = 60;
    private int graceSeconds = 15;
    private int minDistinctSources = 3;

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
}
