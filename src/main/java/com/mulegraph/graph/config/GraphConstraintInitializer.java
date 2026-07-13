package com.mulegraph.graph.config;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.data.neo4j.core.Neo4jClient;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
@Profile("graph-projector")
public class GraphConstraintInitializer implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(GraphConstraintInitializer.class);
    private final Neo4jClient neo4jClient;

    public GraphConstraintInitializer(Neo4jClient neo4jClient) {
        this.neo4jClient = neo4jClient;
    }

    @Override
    public void run(ApplicationArguments args) {
        log.info("Initializing Neo4j graph constraints...");
        
        String[] constraints = {
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Account) REQUIRE n.account_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Device) REQUIRE n.device_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:IPAddress) REQUIRE n.ip_hash IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:ProcessedGraphEvent) REQUIRE n.event_id IS UNIQUE"
        };

        for (String constraint : constraints) {
            neo4jClient.query(constraint).run();
            log.info("Executed constraint: {}", constraint);
        }
        
        log.info("Neo4j graph constraints initialized successfully.");
    }
}
