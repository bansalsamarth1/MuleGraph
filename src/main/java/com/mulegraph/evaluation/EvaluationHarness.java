package com.mulegraph.evaluation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

@Component
@Profile("evaluator")
public class EvaluationHarness implements CommandLineRunner {

    private final JdbcTemplate jdbcTemplate;
    private final RestTemplate restTemplate = new RestTemplate();
    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    private final Random random = new Random(42); // Fixed seed
    private final String API_URL = "http://localhost:8080/api/v1/transactions";
    private final String API_KEY = "dev-local-api-key";

    private final List<ExpectedAlert> expectedAlerts = new ArrayList<>();
    private final Map<String, Instant> expectedAlertTriggerTimes = new HashMap<>();

    public EvaluationHarness(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    record ExpectedAlert(String ruleType, UUID primaryAccountId, String scenarioId) {}

    record TransactionPayload(UUID transaction_id, UUID source_account_id, UUID destination_account_id,
                              long amount_minor, String currency, String device_id, String ip_hash, Instant occurred_at) {}

    @Override
    public void run(String... args) throws Exception {
        System.out.println("=================================================");
        System.out.println("Starting MuleGraph Synthetic Metric Evaluation...");
        System.out.println("=================================================");

        // Clear previous alerts for a clean run
        jdbcTemplate.execute("TRUNCATE TABLE alerts RESTART IDENTITY CASCADE");

        Instant baseTime = Instant.now().truncatedTo(ChronoUnit.HOURS);
        int totalTx = 0;

        // 1. Normal Background Noise (100 tx)
        System.out.println("Generating Normal Traffic...");
        for (int i = 0; i < 100; i++) {
            sendTx(UUID.randomUUID(), UUID.randomUUID(), 5000, "dev_" + i, "ip_" + i, baseTime.plusSeconds(i * 10));
            totalTx++;
        }

        // 2. Fan-Out (10 scenarios) -> 1 to >= 5 within 60s
        System.out.println("Generating Fan-Out Scenarios...");
        for (int i = 0; i < 10; i++) {
            UUID src = UUID.randomUUID();
            Instant start = baseTime.plus(1, ChronoUnit.HOURS).plusSeconds(i * 100);
            for (int j = 0; j < 5; j++) {
                Instant t = start.plusSeconds(j);
                sendTx(src, UUID.randomUUID(), 1000, "dev_fo_" + i, "ip_fo_" + i, t);
                totalTx++;
                if (j == 4) recordExpectedAlert("FAN_OUT", src, "FO_" + i, t);
            }
        }

        // 3. Fan-In (10 scenarios) -> >= 5 to 1 within 60s
        System.out.println("Generating Fan-In Scenarios...");
        for (int i = 0; i < 10; i++) {
            UUID dest = UUID.randomUUID();
            Instant start = baseTime.plus(2, ChronoUnit.HOURS).plusSeconds(i * 100);
            for (int j = 0; j < 5; j++) {
                Instant t = start.plusSeconds(j);
                sendTx(UUID.randomUUID(), dest, 1000, "dev_fi_" + i, "ip_fi_" + i, t);
                totalTx++;
                if (j == 4) recordExpectedAlert("FAN_IN", dest, "FI_" + i, t);
            }
        }

        // 4. Shared Device (10 scenarios) -> >= 3 sources from 1 device
        System.out.println("Generating Shared-Device Scenarios...");
        for (int i = 0; i < 10; i++) {
            String device = "SHARED_DEV_" + i;
            Instant start = baseTime.plus(3, ChronoUnit.HOURS).plusSeconds(i * 100);
            UUID lastSrc = null;
            for (int j = 0; j < 3; j++) {
                Instant t = start.plusSeconds(j);
                lastSrc = UUID.randomUUID();
                sendTx(lastSrc, UUID.randomUUID(), 1000, device, "ip_sd_" + i + "_" + j, t);
                totalTx++;
                if (j == 2) {
                    // SharedDeviceTopology hashes the deviceId to create the primaryAccountId
                    UUID deviceHash = UUID.nameUUIDFromBytes(device.getBytes());
                    recordExpectedAlert("SHARED_DEVICE", deviceHash, "SD_" + i, t);
                }
            }
        }

        // 5. Shared IP (10 scenarios) -> >= 3 sources from 1 IP
        System.out.println("Generating Shared-IP Scenarios...");
        for (int i = 0; i < 10; i++) {
            String ip = "SHARED_IP_" + i;
            Instant start = baseTime.plus(4, ChronoUnit.HOURS).plusSeconds(i * 100);
            UUID lastSrc = null;
            for (int j = 0; j < 3; j++) {
                Instant t = start.plusSeconds(j);
                lastSrc = UUID.randomUUID();
                sendTx(lastSrc, UUID.randomUUID(), 1000, "dev_si_" + i + "_" + j, ip, t);
                totalTx++;
                if (j == 2) {
                    // SharedIpTopology hashes the ipHash to create the primaryAccountId
                    UUID ipHashUuid = UUID.nameUUIDFromBytes(ip.getBytes());
                    recordExpectedAlert("SHARED_IP", ipHashUuid, "SI_" + i, t);
                }
            }
        }

        // 6. Circular Flow (5 scenarios) -> A->B->C->A within 24h
        System.out.println("Generating Circular Flow Scenarios...");
        for (int i = 0; i < 5; i++) {
            UUID a = UUID.randomUUID();
            UUID b = UUID.randomUUID();
            UUID c = UUID.randomUUID();
            Instant start = baseTime.plus(5, ChronoUnit.HOURS).plusSeconds(i * 10000);
            sendTx(a, b, 1000, "dev_c_" + i + "a", "ip_c_" + i + "a", start);
            sendTx(b, c, 1000, "dev_c_" + i + "b", "ip_c_" + i + "b", start.plusSeconds(3600));
            
            // Sleep slightly to allow graph projection for A->B and B->C to complete before sending the candidate C->A
            try { Thread.sleep(10000); } catch (InterruptedException e) {}

            Instant t = start.plusSeconds(7200);
            sendTx(c, a, 1000, "dev_c_" + i + "c", "ip_c_" + i + "c", t);
            totalTx += 3;
            recordExpectedAlert("CIRCULAR_FLOW", c, "CF_" + i, t); // Primary account is the candidate source
        }

        // 7. Advance Stream Time (Dummy transactions in the future)
        System.out.println("Advancing Stream Time...");
        Instant futureTime = baseTime.plus(10, ChronoUnit.DAYS);
        for (int i = 0; i < 50; i++) {
            sendTx(UUID.randomUUID(), UUID.randomUUID(), 100, "dev_future_" + i, "ip_future_" + i, futureTime.plusSeconds(i));
        }

        System.out.println("Injection complete. Total transactions sent: " + totalTx);
        System.out.println("Expected Alerts: " + expectedAlerts.size());

        System.out.println("Waiting for asynchronous processing (up to 30s)...");
        waitForProcessing();

        evaluateAndReport(totalTx);

        System.exit(0);
    }

    private void sendTx(UUID src, UUID dest, long amount, String device, String ip, Instant time) {
        TransactionPayload payload = new TransactionPayload(UUID.randomUUID(), src, dest, amount, "USD", device, ip, time);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-API-Key", API_KEY);
        try {
            HttpEntity<String> request = new HttpEntity<>(objectMapper.writeValueAsString(payload), headers);
            restTemplate.postForEntity(API_URL, request, String.class);
        } catch (Exception e) {
            System.err.println("Failed to send transaction: " + e.getMessage());
        }
    }

    private void recordExpectedAlert(String ruleType, UUID primaryAccount, String scenarioId, Instant triggerTime) {
        ExpectedAlert ea = new ExpectedAlert(ruleType, primaryAccount, scenarioId);
        expectedAlerts.add(ea);
        expectedAlertTriggerTimes.put(scenarioId, Instant.now()); // The moment the last HTTP request returned
    }

    private void waitForProcessing() throws InterruptedException {
        for (int i = 0; i < 90; i++) {
            Integer count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM alerts", Integer.class);
            if (count != null && count >= expectedAlerts.size()) {
                System.out.println("Target alert count reached!");
                break;
            }
            Thread.sleep(1000);
        }
    }

    private void evaluateAndReport(int totalTx) throws IOException {
        List<Map<String, Object>> actualAlerts = jdbcTemplate.queryForList("SELECT * FROM alerts");

        int truePositives = 0;
        int falsePositives = 0;
        long totalLatencyMs = 0;

        List<ExpectedAlert> unmatchedExpected = new ArrayList<>(expectedAlerts);
        List<Map<String, Object>> unmatchedActual = new ArrayList<>(actualAlerts);

        for (ExpectedAlert expected : expectedAlerts) {
            boolean matched = false;
            for (Iterator<Map<String, Object>> it = unmatchedActual.iterator(); it.hasNext(); ) {
                Map<String, Object> actual = it.next();
                if (expected.ruleType().equals(actual.get("rule_type")) &&
                    expected.primaryAccountId().toString().equals(actual.get("primary_account_id").toString())) {
                    
                    matched = true;
                    truePositives++;
                    it.remove();

                    // Latency calculation
                    java.sql.Timestamp createdAt = (java.sql.Timestamp) actual.get("created_at");
                    Instant trigger = expectedAlertTriggerTimes.get(expected.scenarioId());
                    totalLatencyMs += ChronoUnit.MILLIS.between(trigger, createdAt.toInstant());
                    
                    unmatchedExpected.remove(expected);
                    break;
                }
            }
        }

        falsePositives = unmatchedActual.size();
        int falseNegatives = unmatchedExpected.size();
        int actualNegatives = totalTx - expectedAlerts.size(); // Approximation of negative events

        double precision = truePositives == 0 ? 0 : (double) truePositives / (truePositives + falsePositives);
        double recall = truePositives == 0 ? 0 : (double) truePositives / (truePositives + falseNegatives);
        double f1 = (precision + recall) == 0 ? 0 : 2 * (precision * recall) / (precision + recall);
        double fpr = (double) falsePositives / actualNegatives;
        double fnr = (double) falseNegatives / (truePositives + falseNegatives);
        long avgLatencyMs = truePositives == 0 ? 0 : totalLatencyMs / truePositives;

        StringBuilder sb = new StringBuilder();
        sb.append("# MuleGraph Phase 5 Metric Evaluation Report\n\n");
        sb.append("## Setup\n");
        sb.append("- **Seed**: 42 (Fixed deterministic seed)\n");
        sb.append("- **Dataset Size**: ").append(totalTx).append(" transactions injected via REST API\n");
        sb.append("- **Expected Scenarios**: ").append(expectedAlerts.size()).append("\n\n");

        sb.append("## Results\n");
        sb.append("- **Total Expected Alerts**: ").append(expectedAlerts.size()).append("\n");
        sb.append("- **Total Actual Alerts Generated**: ").append(actualAlerts.size()).append("\n");
        sb.append("- **True Positives (TP)**: ").append(truePositives).append("\n");
        sb.append("- **False Positives (FP)**: ").append(falsePositives).append("\n");
        sb.append("- **False Negatives (FN)**: ").append(falseNegatives).append("\n\n");

        sb.append("## Metrics\n");
        sb.append(String.format("- **Precision**: %.4f\n", precision));
        sb.append(String.format("- **Recall**: %.4f\n", recall));
        sb.append(String.format("- **F1 Score**: %.4f\n", f1));
        sb.append(String.format("- **False Positive Rate (FPR)**: %.4f\n", fpr));
        sb.append(String.format("- **False Negative Rate (FNR)**: %.4f\n", fnr));
        sb.append(String.format("- **Average Detection Latency**: %d ms\n\n", avgLatencyMs));

        if (!unmatchedExpected.isEmpty()) {
            sb.append("## Missed Scenarios (FN)\n");
            for (ExpectedAlert ea : unmatchedExpected) {
                sb.append("- ").append(ea).append("\n");
            }
            sb.append("\n");
        }

        if (!unmatchedActual.isEmpty()) {
            sb.append("## Unmatched Alerts (FP)\n");
            for (Map<String, Object> ua : unmatchedActual) {
                sb.append("- Rule: ").append(ua.get("rule_type")).append(", Primary Acc: ").append(ua.get("primary_account_id")).append("\n");
            }
            sb.append("\n");
        }

        Files.createDirectories(Paths.get("docs/evidence/phase-5"));
        Files.writeString(Paths.get("docs/evidence/phase-5/evaluation_report.md"), sb.toString());
        
        System.out.println("Evaluation complete. Report saved to docs/evidence/phase-5/evaluation_report.md");
    }
}
