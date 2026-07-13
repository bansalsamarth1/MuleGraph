package com.mulegraph.fraud.topology;

import com.mulegraph.fraud.config.SharedDeviceRuleProperties;
import com.mulegraph.fraud.domain.FraudCandidateEvent;
import com.mulegraph.ingestion.domain.InternalTransactionEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SharedDeviceTopologyTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, InternalTransactionEvent> inputTopic;
    private TestOutputTopic<String, FraudCandidateEvent> outputTopic;

    @BeforeEach
    void setup() {
        SharedDeviceRuleProperties properties = new SharedDeviceRuleProperties();
        properties.setWindowSeconds(60);
        properties.setGraceSeconds(15);
        properties.setMinDistinctSources(3);

        SharedDeviceTopology topology = new SharedDeviceTopology(properties);
        StreamsBuilder builder = new StreamsBuilder();
        topology.buildTopology(builder);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "shared-device-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        testDriver = new TopologyTestDriver(builder.build(), props);

        inputTopic = testDriver.createInputTopic(
                "transactions.by-device",
                new StringSerializer(),
                new JsonSerializer<>()
        );

        outputTopic = testDriver.createOutputTopic(
                "fraud.candidates",
                new StringDeserializer(),
                new JsonDeserializer<>(FraudCandidateEvent.class)
        );
    }

    @AfterEach
    void teardown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    @Test
    void testTwoDistinctSources_DoesNotTrigger() {
        String deviceId = "device-123";
        Instant now = Instant.now();

        for (int i = 0; i < 2; i++) {
            inputTopic.pipeInput(
                    deviceId,
                    createEvent(UUID.randomUUID(), deviceId),
                    now.plusSeconds(i).toEpochMilli()
            );
        }

        assertTrue(outputTopic.isEmpty(), "Output should be empty for 2 sources");
    }

    @Test
    void testThreeDistinctSources_TriggersExactlyOneCandidate() {
        String deviceId = "device-123";
        Instant now = Instant.now();

        for (int i = 0; i < 3; i++) {
            inputTopic.pipeInput(
                    deviceId,
                    createEvent(UUID.randomUUID(), deviceId),
                    now.plusSeconds(i).toEpochMilli()
            );
        }

        assertEquals(1, outputTopic.getQueueSize(), "Should emit exactly one candidate");
        FraudCandidateEvent candidate = outputTopic.readValue();
        assertEquals(3, candidate.distinctAccountsCount());
        assertEquals("SHARED_DEVICE", candidate.ruleType());
        
        UUID expectedDeviceUuid = UUID.nameUUIDFromBytes(deviceId.getBytes());
        assertEquals(expectedDeviceUuid, candidate.primaryAccountId());
        assertEquals(3, candidate.involvedAccounts().size());
        assertEquals(3, candidate.transactionIds().size());

        // Piping 4th should NOT trigger another candidate for the same window
        inputTopic.pipeInput(
                deviceId,
                createEvent(UUID.randomUUID(), deviceId),
                now.plusSeconds(10).toEpochMilli()
        );
        assertTrue(outputTopic.isEmpty(), "Should not emit a second candidate for the same window");
    }

    private InternalTransactionEvent createEvent(UUID source, String deviceId) {
        return new InternalTransactionEvent(
                UUID.randomUUID(),
                UUID.randomUUID(),
                1,
                UUID.randomUUID().toString(),
                source,
                UUID.randomUUID(),
                10000L,
                "INR",
                deviceId,
                "hash-1",
                Instant.now(),
                Instant.now()
        );
    }
}
