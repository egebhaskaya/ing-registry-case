package com.example.compliance;

import org.springframework.boot.SpringApplication;

public class TestComplianceApplication {

	public static void main(String[] args) {
		SpringApplication.from(ComplianceApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
