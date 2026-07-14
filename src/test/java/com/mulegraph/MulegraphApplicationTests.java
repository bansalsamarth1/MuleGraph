package com.mulegraph;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest(properties = {
    "spring.kafka.streams.state.dir=/tmp/kafka-streams-${random.uuid}",
    "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.neo4j.Neo4jAutoConfiguration,org.springframework.boot.autoconfigure.data.neo4j.Neo4jDataAutoConfiguration,org.springframework.boot.autoconfigure.data.neo4j.Neo4jRepositoriesAutoConfiguration"
})
@Testcontainers
class MulegraphApplicationTests {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine");

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.1"))
            .withEnv("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:9093,BROKER://0.0.0.0:9092")
            .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
            .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER");

    @org.springframework.test.context.DynamicPropertySource
    static void kafkaProperties(org.springframework.test.context.DynamicPropertyRegistry registry) {
        registry.add("KAFKA_BOOTSTRAP_SERVERS", kafka::getBootstrapServers);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

	@Test
	void contextLoads() {
	}

}
