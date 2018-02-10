package org.hljcma.di;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan("org.hljcma.di.mapper")
public class DiApplication {

	public static void main(String[] args) {
		SpringApplication.run(DiApplication.class, args);
	}
}
