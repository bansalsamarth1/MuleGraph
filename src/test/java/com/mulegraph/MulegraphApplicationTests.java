package com.mulegraph;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@Disabled("Requires Postgres and Kafka to load full context")
@SpringBootTest
class MulegraphApplicationTests {

	@Test
	void contextLoads() {
	}

}
