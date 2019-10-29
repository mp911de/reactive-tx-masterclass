package org.neo4j.sdn_rx;

import static org.neo4j.driver.Values.*;

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.function.Function;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.reactive.RxStatementResult;
import org.neo4j.driver.reactive.RxTransaction;
import org.neo4j.driver.reactive.RxTransactionWork;
import org.neo4j.springframework.data.core.ReactiveNeo4jClient;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;


@SpringBootTest // New @DataNeo4jTest in beta02
class SdnRxTest {

	@Autowired
	private PersonRepository repository;

	@Autowired
	private ReactiveNeo4jClient neo4jClient;

	@Autowired
	private ReactiveTransactionManager reactiveTransactionManager;

	@Test
	void repositoryShouldWork() {

		StepVerifier.create(repository.findAll())
			.thenConsumeWhile(p -> true)
			.verifyComplete();
	}

	@Test
	void clientWithExplicitTransactions() {

		TransactionalOperator transactionalOperator = TransactionalOperator.create(reactiveTransactionManager);

		Flux<String> movieTitles = transactionalOperator
			.transactional(neo4jClient.query("MATCH (m:Movie) RETURN m.title").fetchAs(String.class).all());

		StepVerifier.create(movieTitles)
			.thenConsumeWhile(t -> true, System.out::println)
			.verifyComplete();
	}
}
