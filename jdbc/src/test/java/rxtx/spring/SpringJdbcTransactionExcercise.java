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

import rxtx.Rows;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Service;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionOperations;

/**
 * Tests explaining JDBC transactions using Spring Framework.
 */
@SpringJUnitConfig
final class SpringJdbcTransactionExcercise {

	@BeforeEach
	void setUp(@Autowired JdbcOperations operations) {

		operations.execute("DROP TABLE IF EXISTS person");
		operations.execute("DROP TABLE IF EXISTS person_event");

		operations
				.execute("CREATE TABLE person " + "(id INT PRIMARY KEY, first_name VARCHAR(255), last_name VARCHAR(255))");
		operations.execute("CREATE TABLE person_event "
				+ "(id INT PRIMARY KEY, first_name VARCHAR(255), last_name VARCHAR(255), action VARCHAR(255))");
	}

	@Test
	void autoCommit(@Autowired JdbcOperations operations) {

	}

	@Test
	void autoCommitWithFailure(@Autowired JdbcOperations operations) {

		SqlRowSet resultSet = operations.queryForRowSet("SELECT * FROM person");
		System.out.println("Rows in person");
		Rows.print(resultSet);

		resultSet = operations.queryForRowSet("SELECT * FROM person_event");
		System.out.println("Rows in person_event");
		Rows.print(resultSet);
	}

	@Test
	void programmaticTransactionalWithRollback(@Autowired JdbcOperations operations,
			@Autowired TransactionOperations tx) {

		SqlRowSet resultSet = operations.queryForRowSet("SELECT * FROM person");
		System.out.println("Rows in person");
		Rows.print(resultSet);

		resultSet = operations.queryForRowSet("SELECT * FROM person_event");
		System.out.println("Rows in person_event");
		Rows.print(resultSet);
	}

	@Test
	void atTransactionalWithRollback(@Autowired JdbcOperations operations,
			@Autowired TransactionalService transactionalService) {

		SqlRowSet resultSet = operations.queryForRowSet("SELECT * FROM person");
		System.out.println("Rows in person");
		Rows.print(resultSet);

		resultSet = operations.queryForRowSet("SELECT * FROM person_event");
		System.out.println("Rows in person_event");
		Rows.print(resultSet);
	}

	@SpringBootApplication
	@ComponentScan
	static class TestConfiguration {}

	@Service
	@Transactional
	public static class TransactionalService {

		private final JdbcOperations operations;

		public TransactionalService(JdbcOperations operations) {
			this.operations = operations;
		}

		public void insert() {
			operations.execute("INSERT INTO person VALUES(1, 'Jesse', 'Pinkman')");
			operations.execute("INSERT INTO person_event VALUES(1, 'Jesse', 'Pinkman', 'CREATED')");
		}

		public void delete() {
			operations.execute("DELETE FROM person WHERE id = 1");
			operations.execute("INSERT INTO person_event VALUES(1, 'Jesse', 'Pinkman', 'DELETED')");
		}
	}
}
