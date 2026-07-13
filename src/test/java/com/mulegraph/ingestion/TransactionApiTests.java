package com.mulegraph.ingestion;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.UUID;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.UUID;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest(properties = {
        "spring.flyway.enabled=false",
        "spring.kafka.listener.auto-startup=false",
        "spring.kafka.streams.auto-startup=false",
        "management.health.kafka.enabled=false"
})
@AutoConfigureMockMvc
public class TransactionApiTests {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private com.mulegraph.ingestion.port.TransactionPublisher transactionPublisher;

    @MockitoBean
    private javax.sql.DataSource dataSource;

    private static final String API_KEY = "dev-local-api-key";

    private String buildValidPayload() {
        return """
                {
                  "transaction_id": "%s",
                  "source_account_id": "%s",
                  "destination_account_id": "%s",
                  "amount_minor": 125050,
                  "currency": "INR",
                  "device_id": "device-123",
                  "ip_hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                  "occurred_at": "2026-07-11T10:20:30Z"
                }
                """.formatted(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
    }

    @Test
    void testMissingApiKey_returns401() throws Exception {
        mockMvc.perform(post("/api/v1/transactions")
                .contentType(MediaType.APPLICATION_JSON)
                .content(buildValidPayload()))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void testInvalidApiKey_returns403() throws Exception {
        mockMvc.perform(post("/api/v1/transactions")
                .header("X-API-Key", "wrong-key")
                .contentType(MediaType.APPLICATION_JSON)
                .content(buildValidPayload()))
                .andExpect(status().isForbidden());
    }

    @Test
    void testValidPayload_returns202() throws Exception {
        mockMvc.perform(post("/api/v1/transactions")
                .header("X-API-Key", API_KEY)
                .contentType(MediaType.APPLICATION_JSON)
                .content(buildValidPayload()))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("ACCEPTED"));
    }

    @Test
    void testZeroAmount_returns400() throws Exception {
        String payload = buildValidPayload().replace("125050", "0");
        mockMvc.perform(post("/api/v1/transactions")
                .header("X-API-Key", API_KEY)
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
                .andExpect(status().isBadRequest());
    }

    @Test
    void testSameSourceAndDestination_returns400() throws Exception {
        String uuid = UUID.randomUUID().toString();
        String payload = """
                {
                  "transaction_id": "%s",
                  "source_account_id": "%s",
                  "destination_account_id": "%s",
                  "amount_minor": 1000,
                  "currency": "USD",
                  "occurred_at": "2026-07-11T10:20:30Z"
                }
                """.formatted(UUID.randomUUID(), uuid, uuid);
        mockMvc.perform(post("/api/v1/transactions")
                .header("X-API-Key", API_KEY)
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
                .andExpect(status().isBadRequest());
    }

    @Test
    void testMalformedCurrency_returns400() throws Exception {
        String payload = buildValidPayload().replace("\"INR\"", "\"INRA\""); // 4 letters
        mockMvc.perform(post("/api/v1/transactions")
                .header("X-API-Key", API_KEY)
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
                .andExpect(status().isBadRequest());
    }

    @Test
    void testMalformedBody_returns400() throws Exception {
        mockMvc.perform(post("/api/v1/transactions")
                .header("X-API-Key", API_KEY)
                .contentType(MediaType.APPLICATION_JSON)
                .content("{ invalid json"))
                .andExpect(status().isBadRequest());
    }
}
