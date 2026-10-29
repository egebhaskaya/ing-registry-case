package com.example.registry;

import org.springframework.boot.SpringApplication;

public class TestRegistryApplication {

	public static void main(String[] args) {
		SpringApplication.from(RegistryApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
