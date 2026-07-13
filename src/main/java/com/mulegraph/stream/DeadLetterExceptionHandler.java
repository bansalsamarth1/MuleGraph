package com.mulegraph.stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class DeadLetterExceptionHandler implements DeserializationExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterExceptionHandler.class);
    private KafkaProducer<byte[], byte[]> producer;
    private static final String DLQ_TOPIC = "processing.dead-letter";

    @Override
    public void configure(Map<String, ?> configs) {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configs.get("bootstrap.servers"));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producer = new KafkaProducer<>(producerProps);
    }

    @Override
    public DeserializationHandlerResponse handle(ProcessorContext context,
                                                 org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> record,
                                                 Exception exception) {
        log.error("Deserialization error for topic {}, partition {}, offset {}", 
                  record.topic(), record.partition(), record.offset(), exception);

        try {
            ProducerRecord<byte[], byte[]> dlqRecord = new ProducerRecord<>(
                    DLQ_TOPIC, null, record.timestamp(), record.key(), record.value(), record.headers());
            dlqRecord.headers().add("deserialization-error", exception.getMessage().getBytes());
            producer.send(dlqRecord);
        } catch (Exception e) {
            log.error("Failed to send record to DLQ topic {}", DLQ_TOPIC, e);
        }
        
        return DeserializationHandlerResponse.CONTINUE;
    }
}
