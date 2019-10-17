/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package rxtx;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

/**
 * Extension providing a JDBC {@link Connection}.
 */
final class JdbcConnectionExtension implements AfterEachCallback, ParameterResolver {

	private static final ExtensionContext.Namespace JDBC = ExtensionContext.Namespace.create("JDBC");

	@Override
	public void afterEach(ExtensionContext context) throws Exception {

		ExtensionContext.Store store = context.getStore(JDBC);
		Statement statement = store.get(Statement.class, Statement.class);
		if (statement != null) {
			store.remove(Statement.class);
			statement.close();
		}

		Connection connection = store.get(Connection.class, Connection.class);
		if (connection != null) {
			store.remove(Connection.class);
			connection.close();
		}
	}

	@Override
	public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
			throws ParameterResolutionException {
		return parameterContext.getParameter().getType().isAssignableFrom(Connection.class)
				|| parameterContext.getParameter().getType().isAssignableFrom(Statement.class);
	}

	@Override
	public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
			throws ParameterResolutionException {

		ExtensionContext.Store store = extensionContext.getStore(JDBC);

		if (parameterContext.getParameter().getType().isAssignableFrom(Connection.class)) {
			return getConnection(store);
		}

		if (parameterContext.getParameter().getType().isAssignableFrom(Statement.class)) {
			return getStatement(store);
		}

		throw new ParameterResolutionException("¯\\_(ツ)_/¯");
	}

	private Connection getConnection(ExtensionContext.Store store) {

		Connection connection = store.get(Connection.class, Connection.class);
		if (connection == null) {

			try {
				connection = DriverManager
						.getConnection("jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=true", "sa", "sa");
			} catch (SQLException e) {
				throw new ParameterResolutionException("Cannot create connection", e);
			}

			store.put(Connection.class, connection);
		}

		return connection;
	}

	private Statement getStatement(ExtensionContext.Store store) {

		Statement statement = store.get(Statement.class, Statement.class);
		if (statement == null) {

			try {
				statement = getConnection(store).createStatement();
				store.put(Statement.class, statement);
			} catch (SQLException e) {
				throw new ParameterResolutionException("Cannot create connection", e);
			}
		}

		return statement;
	}
}
