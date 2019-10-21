/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package rxtx.spring;

import io.r2dbc.h2.CloseableConnectionFactory;
import io.r2dbc.h2.H2ConnectionFactory;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Result;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import rxtx.RowPrinter;
import rxtx.extension.R2dbcH2ConnectionExtension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.data.r2dbc.connectionfactory.R2dbcTransactionManager;
import org.springframework.data.r2dbc.core.DatabaseClient;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.reactive.TransactionalOperator;

/**
 * Tests explaining R2DBC transactions using Spring Framework and Spring Data R2DBC.
 */
@ExtendWith(R2dbcH2ConnectionExtension.class)
@SpringBootTest(classes = SpringDataR2dbcTransactionExcercise.TestConfiguration.class)
final class SpringDataR2dbcTransactionExcercise {

	@BeforeEach
	void setUp(Connection connection) {

		Flux<Integer> dropPerson = executeUpdate(connection, "DROP TABLE IF EXISTS person");
		Flux<Integer> dropPersonEvent = executeUpdate(connection, "DROP TABLE IF EXISTS person_event");

		dropPerson.thenMany(dropPersonEvent).then().as(StepVerifier::create).verifyComplete();

		Flux<Integer> createPerson = executeUpdate(connection,
				"CREATE TABLE person " + "(id INT PRIMARY KEY, first_name VARCHAR(255), last_name VARCHAR(255))");
		Flux<Integer> createPersonEvent = executeUpdate(connection, "CREATE TABLE person_event "
				+ "(id INT PRIMARY KEY, first_name VARCHAR(255), last_name VARCHAR(255), action VARCHAR(255))");

		createPerson.thenMany(createPersonEvent).then().as(StepVerifier::create).verifyComplete();
	}

	@Test
	void autoCommit(@Autowired DatabaseClient databaseClient) {

		Mono<Void> insertPerson = Mono.empty();
		Mono<Void> insertPersonEvent = Mono.empty();

		insertPerson.then(insertPersonEvent).then().as(StepVerifier::create).verifyComplete();
	}

	@Test
	void autoCommitWithFailure(Connection connection, @Autowired DatabaseClient databaseClient) {

		Mono<Void> insertPerson = Mono.empty();
		Mono<Void> insertPersonEvent = Mono.empty();

		insertPerson.then(insertPersonEvent).then().as(StepVerifier::create).verifyComplete();

		Mono<Void> deletePerson = Mono.empty();
		Mono<Void> insertPersonDeleteEvent = Mono.empty();

		Flux<Result> person = executeQuery(connection, "SELECT * FROM person");
		System.out.println("Rows in person");
		person.flatMap(new RowPrinter()).as(StepVerifier::create).verifyComplete();

		Flux<Result> person_event = executeQuery(connection, "SELECT * FROM person_event");
		System.out.println("Rows in person_event");
		person_event.flatMap(new RowPrinter()).as(StepVerifier::create).verifyComplete();
	}

	@Test
	void programmaticTransactionalWithRollback(Connection connection, @Autowired DatabaseClient databaseClient,
			@Autowired TransactionalOperator rxtx) {

		Mono<Void> insertPerson = Mono.empty();
		Mono<Void> insertPersonEvent = Mono.empty();

		insertPerson.then(insertPersonEvent).then().as(StepVerifier::create).verifyComplete();

		Mono<Void> deletePerson = Mono.empty();
		Mono<Void> insertPersonDeleteEvent = Mono.empty();

		Flux<Result> person = executeQuery(connection, "SELECT * FROM person");
		System.out.println("Rows in person");
		person.flatMap(new RowPrinter()).as(StepVerifier::create).verifyComplete();

		Flux<Result> person_event = executeQuery(connection, "SELECT * FROM person_event");
		System.out.println("Rows in person_event");
		person_event.flatMap(new RowPrinter()).as(StepVerifier::create).verifyComplete();
	}

	@Test
	void atTransactionalWithRollback(Connection connection, @Autowired TransactionalService transactionalService) {

		Flux<Result> person = executeQuery(connection, "SELECT * FROM person");
		System.out.println("Rows in person");
		person.flatMap(new RowPrinter()).as(StepVerifier::create).verifyComplete();

		Flux<Result> person_event = executeQuery(connection, "SELECT * FROM person_event");
		System.out.println("Rows in person_event");
		person_event.flatMap(new RowPrinter()).as(StepVerifier::create).verifyComplete();
	}

	private Flux<Integer> executeUpdate(Connection connection, String sql) {
		return Flux.from(connection.createStatement(sql).execute()).flatMap(Result::getRowsUpdated);
	}

	private Flux<Result> executeQuery(Connection connection, String sql) {
		return Flux.from(connection.createStatement(sql).execute());
	}

	@SpringBootApplication
	static class TestConfiguration {

		@Bean
		CloseableConnectionFactory connectionFactory() {
			return H2ConnectionFactory.inMemory("R2dbcConnectionExtension");
		}

		@Bean
		DatabaseClient databaseClient(ConnectionFactory connectionFactory) {
			return DatabaseClient.create(connectionFactory);
		}

		@Bean
		R2dbcTransactionManager transactionManager(ConnectionFactory connectionFactory) {
			return new R2dbcTransactionManager(connectionFactory);
		}

		@Bean
		TransactionalService transactionalService(DatabaseClient databaseClient) {
			return new TransactionalService(databaseClient);
		}

	}

	@Transactional
	public static class TransactionalService {

		private final DatabaseClient databaseClient;

		public TransactionalService(DatabaseClient databaseClient) {
			this.databaseClient = databaseClient;
		}

		public Mono<Void> insert() {
			Mono<Void> insertPerson = databaseClient.execute("INSERT INTO person VALUES(1, 'Jesse', 'Pinkman')").then();
			Mono<Void> insertPersonEvent = databaseClient
					.execute("INSERT INTO person_event VALUES(1, 'Jesse', 'Pinkman', 'CREATED')").then();
			return insertPerson.then(insertPersonEvent).then();
		}

		public Mono<Void> delete() {
			Mono<Void> deletePerson = databaseClient.execute("DELETE FROM person WHERE id = 1").then();
			Mono<Void> insertPersonDeleteEvent = databaseClient
					.execute("INSERT INTO person_event VALUES(1, 'Jesse', 'Pinkman', 'DELETED')").then();
			return deletePerson.then(insertPersonDeleteEvent).then();
		}

	}

}
