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
package rxtx;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests explaining JDBC transactions using JDBC API.
 */
@ExtendWith(JdbcConnectionExtension.class)
final class JdbcTransactionExcercise {

	@BeforeEach
	void setUp(Connection connection) throws SQLException {

		Statement statement = connection.createStatement();
		statement.executeUpdate(
				"CREATE TABLE person " + "(id INT PRIMARY KEY, first_name VARCHAR(255), last_name VARCHAR(255))");
		statement.executeUpdate("CREATE TABLE person_event "
				+ "(id INT PRIMARY KEY, first_name VARCHAR(255), last_name VARCHAR(255), action VARCHAR(255))");
		statement.close();
	}

	@Test
	void autoCommit(Statement statement) throws SQLException {

	}

	@Test
	void autoCommitWithFailure(Statement statement) throws SQLException {

		try (ResultSet resultSet = statement.executeQuery("SELECT * FROM person")) {
			System.out.println("Row in person");
			Rows.print(resultSet);
		}

		try (ResultSet resultSet = statement.executeQuery("SELECT * FROM person_event")) {
			System.out.println("Rows in person_event");
			Rows.print(resultSet);
		}
	}

	@Test
	void transactionalWithFailure(Connection connection, Statement statement) throws SQLException {

		try (ResultSet resultSet = statement.executeQuery("SELECT * FROM person")) {
			System.out.println("Row in person");
			Rows.print(resultSet);
		}

		try (ResultSet resultSet = statement.executeQuery("SELECT * FROM person_event")) {
			System.out.println("Rows in person_event");
			Rows.print(resultSet);
		}
	}
}
