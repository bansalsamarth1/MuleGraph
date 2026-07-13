package com.mulegraph.fraud.domain;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class FanInState {
    private Set<UUID> sources = new HashSet<>();
    private Set<UUID> transactionIds = new HashSet<>();
    private long transactionCount = 0;
    private long totalAmountMinor = 0;
    private boolean candidateEmitted = false;
    private boolean thresholdCrossed = false;

    public FanInState() {
    }

    public FanInState(Set<UUID> sources, Set<UUID> transactionIds, long transactionCount, long totalAmountMinor, boolean candidateEmitted) {
        this.sources = sources;
        this.transactionIds = transactionIds;
        this.transactionCount = transactionCount;
        this.totalAmountMinor = totalAmountMinor;
        this.candidateEmitted = candidateEmitted;
    }

    public Set<UUID> getSources() {
        return sources;
    }

    public void setSources(Set<UUID> sources) {
        this.sources = sources;
    }

    public Set<UUID> getTransactionIds() {
        return transactionIds;
    }

    public void setTransactionIds(Set<UUID> transactionIds) {
        this.transactionIds = transactionIds;
    }

    public long getTransactionCount() {
        return transactionCount;
    }

    public void setTransactionCount(long transactionCount) {
        this.transactionCount = transactionCount;
    }

    public long getTotalAmountMinor() {
        return totalAmountMinor;
    }

    public void setTotalAmountMinor(long totalAmountMinor) {
        this.totalAmountMinor = totalAmountMinor;
    }

    public boolean isCandidateEmitted() {
        return candidateEmitted;
    }

    public void setCandidateEmitted(boolean candidateEmitted) {
        this.candidateEmitted = candidateEmitted;
    }

    public boolean isThresholdCrossed() {
        return thresholdCrossed;
    }

    public void setThresholdCrossed(boolean thresholdCrossed) {
        this.thresholdCrossed = thresholdCrossed;
    }
}
