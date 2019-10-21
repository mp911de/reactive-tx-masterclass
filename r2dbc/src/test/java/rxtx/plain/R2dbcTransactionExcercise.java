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
package rxtx.plain;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Result;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import rxtx.RowPrinter;
import rxtx.extension.R2dbcConnectionExtension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests explaining R2DBC transactions using R2DBC API.
 */
@ExtendWith(R2dbcConnectionExtension.class)
final class R2dbcTransactionExcercise {

	@BeforeEach
	void setUp(Connection connection) {

		Flux<Integer> createPerson = executeUpdate(connection,
				"CREATE TABLE person " + "(id INT PRIMARY KEY, first_name VARCHAR(255), last_name VARCHAR(255))");
		Flux<Integer> createPersonEvent = executeUpdate(connection, "CREATE TABLE person_event "
				+ "(id INT PRIMARY KEY, first_name VARCHAR(255), last_name VARCHAR(255), action VARCHAR(255))");

		createPerson.thenMany(createPersonEvent).then().as(StepVerifier::create).verifyComplete();
	}

	@Test
	void autoCommit(Connection connection) {

		Flux<Integer> insertPerson = Flux.empty();
		Flux<Integer> insertPersonEvent = Flux.empty();

		insertPerson.thenMany(insertPersonEvent).then().as(StepVerifier::create).verifyComplete();
	}

	@Test
	void autoCommitWithFailure(Connection connection) {

		Flux<Integer> insertPerson = Flux.empty();
		Flux<Integer> insertPersonEvent = Flux.empty();

		Flux<Result> person = executeQuery(connection, "SELECT * FROM person");
		System.out.println("Rows in person");
		person.flatMap(new RowPrinter()).as(StepVerifier::create).verifyComplete();

		Flux<Result> person_event = executeQuery(connection, "SELECT * FROM person_event");
		System.out.println("Rows in person_event");
		person_event.flatMap(new RowPrinter()).as(StepVerifier::create).verifyComplete();
	}

	@Test
	void transactionalWithFailure(Connection connection) {

		Flux<Integer> insertPerson = Flux.empty();
		Flux<Integer> insertPersonEvent = Flux.empty();

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
}
