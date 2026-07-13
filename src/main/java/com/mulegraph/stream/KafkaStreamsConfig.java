package com.mulegraph.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import org.springframework.beans.factory.annotation.Value;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${spring.kafka.streams.state.dir:/tmp/kafka-streams}")
    private String stateDir;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "mulegraph-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TransactionTimestampExtractor.class.getName());
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public org.apache.kafka.clients.admin.NewTopic rawTopic() {
        return org.springframework.kafka.config.TopicBuilder.name("transactions.raw").partitions(1).replicas(1).build();
    }

    @Bean
    public org.apache.kafka.clients.admin.NewTopic validatedTopic() {
        return org.springframework.kafka.config.TopicBuilder.name("transactions.validated").partitions(1).replicas(1).build();
    }

    @Bean
    public org.apache.kafka.clients.admin.NewTopic invalidTopic() {
        return org.springframework.kafka.config.TopicBuilder.name("transactions.invalid").partitions(1).replicas(1).build();
    }

    @Bean
    public org.apache.kafka.clients.admin.NewTopic sourceTopic() {
        return org.springframework.kafka.config.TopicBuilder.name("transactions.by-source").partitions(1).replicas(1).build();
    }

    @Bean
    public org.apache.kafka.clients.admin.NewTopic destinationTopic() {
        return org.springframework.kafka.config.TopicBuilder.name("transactions.by-destination").partitions(1).replicas(1).build();
    }

    @Bean
    public org.apache.kafka.clients.admin.NewTopic deviceTopic() {
        return org.springframework.kafka.config.TopicBuilder.name("activity.by-device").partitions(1).replicas(1).build();
    }

    @Bean
    public org.apache.kafka.clients.admin.NewTopic ipTopic() {
        return org.springframework.kafka.config.TopicBuilder.name("activity.by-ip").partitions(1).replicas(1).build();
    }

    @Bean
    public org.apache.kafka.clients.admin.NewTopic fraudCandidatesTopic() {
        return org.springframework.kafka.config.TopicBuilder.name("fraud.candidates").partitions(1).replicas(1).build();
    }

    @Bean
    public org.apache.kafka.clients.admin.NewTopic fraudAlertsTopic() {
        return org.springframework.kafka.config.TopicBuilder.name("fraud.alerts").partitions(1).replicas(1).build();
    }

    @Bean
    public org.apache.kafka.clients.admin.NewTopic deadLetterTopic() {
        return org.springframework.kafka.config.TopicBuilder.name("processing.dead-letter").partitions(1).replicas(1).build();
    }

    @Bean
    public org.apache.kafka.clients.admin.NewTopic graphUpdatesTopic() {
        return org.springframework.kafka.config.TopicBuilder.name("graph.updates").partitions(1).replicas(1).build();
    }

    @Bean
    public org.apache.kafka.clients.admin.NewTopic graphUpdatesDltTopic() {
        return org.springframework.kafka.config.TopicBuilder.name("graph.updates.DLT").partitions(1).replicas(1).build();
    }
}
