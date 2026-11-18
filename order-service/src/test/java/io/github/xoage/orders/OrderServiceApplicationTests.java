package io.github.xoage.orders;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest(properties = "ORDERS_DB_PASSWORD=unused")
@Import(TestcontainersConfiguration.class)
class OrderServiceApplicationTests {

	@Test
	void contextLoads() {
	}
}
