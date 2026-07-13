package com.mulegraph.stream;

import com.mulegraph.ingestion.domain.InternalTransactionEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
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

class TransactionRekeyingTopologyTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, InternalTransactionEvent> inputTopic;
    private TestOutputTopic<String, InternalTransactionEvent> sourceTopic;
    private TestOutputTopic<String, InternalTransactionEvent> destinationTopic;
    private TestOutputTopic<String, InternalTransactionEvent> deviceTopic;
    private TestOutputTopic<String, InternalTransactionEvent> ipTopic;

    @BeforeEach
    void setup() {
        StreamsBuilder builder = new StreamsBuilder();
        TransactionRekeyingTopology topology = new TransactionRekeyingTopology();
        topology.buildTopology(builder);
        Topology topo = builder.build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TransactionTimestampExtractor.class.getName());

        testDriver = new TopologyTestDriver(topo, props);

        JsonSerializer<InternalTransactionEvent> serializer = new JsonSerializer<>();
        JsonDeserializer<InternalTransactionEvent> deserializer = new JsonDeserializer<>(InternalTransactionEvent.class);
        deserializer.addTrustedPackages("*");

        inputTopic = testDriver.createInputTopic(
                "transactions.validated",
                new StringSerializer(),
                serializer
        );

        sourceTopic = testDriver.createOutputTopic(
                "transactions.by-source",
                new StringDeserializer(),
                deserializer
        );

        destinationTopic = testDriver.createOutputTopic(
                "transactions.by-destination",
                new StringDeserializer(),
                deserializer
        );
        
        deviceTopic = testDriver.createOutputTopic(
                "transactions.by-device",
                new StringDeserializer(),
                deserializer
        );

        ipTopic = testDriver.createOutputTopic(
                "transactions.by-ip",
                new StringDeserializer(),
                deserializer
        );
    }

    @AfterEach
    void tearDown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    @Test
    void shouldRekeyEventsCorrectlyAndExtractTimestamp() {
        // Given
        UUID source = UUID.randomUUID();
        UUID destination = UUID.randomUUID();
        String deviceId = "device-123";
        String ipHash = "hash-123";
        Instant occurredAt = Instant.now().minusSeconds(10);

        InternalTransactionEvent event = new InternalTransactionEvent(
                UUID.randomUUID(),
                UUID.randomUUID(),
                1,
                UUID.randomUUID().toString(),
                source,
                destination,
                10000L,
                "USD",
                deviceId,
                ipHash,
                occurredAt,
                Instant.now()
        );

        // When
        inputTopic.pipeInput("null-key", event);

        // Then
        var sourceRecord = sourceTopic.readRecord();
        assertEquals(source.toString(), sourceRecord.key());
        assertEquals(event.eventId(), sourceRecord.value().eventId());
        assertEquals(occurredAt.toEpochMilli(), sourceRecord.timestamp(), "Timestamp should be extracted from occurredAt");

        var destinationRecord = destinationTopic.readRecord();
        assertEquals(destination.toString(), destinationRecord.key());
        assertEquals(event.eventId(), destinationRecord.value().eventId());

        var deviceRecord = deviceTopic.readRecord();
        assertEquals(deviceId, deviceRecord.key());
        assertEquals(event.eventId(), deviceRecord.value().eventId());

        var ipRecord = ipTopic.readRecord();
        assertEquals(ipHash, ipRecord.key());
        assertEquals(event.eventId(), ipRecord.value().eventId());
    }
}
