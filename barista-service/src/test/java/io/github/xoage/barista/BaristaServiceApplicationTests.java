package io.github.xoage.barista;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
class BaristaServiceApplicationTests {

	@Test
	void contextLoads() {
	}

}
