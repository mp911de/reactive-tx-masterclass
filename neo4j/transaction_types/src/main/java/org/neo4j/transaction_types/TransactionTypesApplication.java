package org.neo4j.transaction_types;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class TransactionTypesApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(TransactionTypesApplication.class, args);
	}



	@Override
	public void run(String... args) throws Exception {

	}
}
