package com.mulegraph;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

// @Disabled removed
@SpringBootTest(properties = {
    "spring.kafka.streams.state.dir=/tmp/kafka-streams-${random.uuid}"
})
class MulegraphApplicationTests {

	@Test
	void contextLoads() {
	}

}
