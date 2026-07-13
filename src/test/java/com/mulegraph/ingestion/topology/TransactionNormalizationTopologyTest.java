package com.mulegraph.ingestion.topology;

import com.mulegraph.ingestion.domain.InternalTransactionEvent;
import com.mulegraph.ingestion.domain.InvalidTransactionRecord;
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

import static org.junit.jupiter.api.Assertions.*;

class TransactionNormalizationTopologyTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, InternalTransactionEvent> inputTopic;
    private TestOutputTopic<String, InternalTransactionEvent> validTopic;
    private TestOutputTopic<String, InvalidTransactionRecord> invalidTopic;

    @BeforeEach
    void setup() {
        TransactionNormalizationTopology topology = new TransactionNormalizationTopology();
        StreamsBuilder builder = new StreamsBuilder();
        topology.buildTopology(builder);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        testDriver = new TopologyTestDriver(builder.build(), props);

        JsonSerializer<InternalTransactionEvent> serializer = new JsonSerializer<>();
        JsonDeserializer<InternalTransactionEvent> deserializer = new JsonDeserializer<>(InternalTransactionEvent.class);
        deserializer.addTrustedPackages("*");

        JsonDeserializer<InvalidTransactionRecord> invalidDeserializer = new JsonDeserializer<>(InvalidTransactionRecord.class);
        invalidDeserializer.addTrustedPackages("*");

        inputTopic = testDriver.createInputTopic(
                "transactions.raw",
                new StringSerializer(),
                serializer
        );

        validTopic = testDriver.createOutputTopic(
                "transactions.validated",
                new StringDeserializer(),
                deserializer
        );

        invalidTopic = testDriver.createOutputTopic(
                "transactions.invalid",
                new StringDeserializer(),
                invalidDeserializer
        );
    }

    @AfterEach
    void teardown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    private InternalTransactionEvent createEvent(UUID source, UUID dest, long amount, String currency, int schema) {
        return new InternalTransactionEvent(
                UUID.randomUUID(),
                UUID.randomUUID(),
                schema,
                "correlation-123",
                source,
                dest,
                amount,
                currency,
                "device-1",
                "ip-1",
                Instant.now(),
                Instant.now()
        );
    }

    @Test
    void testValidEvent_RoutesToValidatedTopic() {
        UUID source = UUID.randomUUID();
        InternalTransactionEvent event = createEvent(source, UUID.randomUUID(), 100, "USD", 1);
        
        inputTopic.pipeInput(source.toString(), event);
        
        assertFalse(validTopic.isEmpty());
        assertTrue(invalidTopic.isEmpty());
        
        InternalTransactionEvent validOut = validTopic.readValue();
        assertEquals(event.eventId(), validOut.eventId());
    }

    @Test
    void testInvalidEvent_NegativeAmount_RoutesToInvalidTopic() {
        UUID source = UUID.randomUUID();
        InternalTransactionEvent event = createEvent(source, UUID.randomUUID(), -100, "USD", 1);
        
        inputTopic.pipeInput(source.toString(), event);
        
        assertTrue(validTopic.isEmpty());
        assertFalse(invalidTopic.isEmpty());
        
        InvalidTransactionRecord invalidOut = invalidTopic.readValue();
        assertEquals(event.eventId(), invalidOut.event().eventId());
        assertEquals("Amount must be positive", invalidOut.failureReason());
    }

    @Test
    void testInvalidEvent_SameSourceAndDest_RoutesToInvalidTopic() {
        UUID source = UUID.randomUUID();
        InternalTransactionEvent event = createEvent(source, source, 100, "USD", 1);
        
        inputTopic.pipeInput(source.toString(), event);
        
        assertTrue(validTopic.isEmpty());
        assertFalse(invalidTopic.isEmpty());
        
        InvalidTransactionRecord invalidOut = invalidTopic.readValue();
        assertEquals("Source equals destination", invalidOut.failureReason());
    }
}
