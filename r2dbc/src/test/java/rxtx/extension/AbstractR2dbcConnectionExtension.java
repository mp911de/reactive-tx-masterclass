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
package rxtx.extension;

import io.r2dbc.spi.Closeable;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

public abstract class AbstractR2dbcConnectionExtension implements AfterEachCallback, ParameterResolver {

	private static final ExtensionContext.Namespace R2DBC = ExtensionContext.Namespace.create("R2DBC");

	@Override
	public void afterEach(ExtensionContext context) {

		ExtensionContext.Store store = context.getStore(R2DBC);

		Connection connection = store.get(Connection.class, Connection.class);
		if (connection != null) {
			store.remove(Connection.class);
			StepVerifier.create(connection.close()).verifyComplete();
		}

		ConnectionFactory connectionFactory = store.get(ConnectionFactory.class, ConnectionFactory.class);
		if (connectionFactory != null) {
			store.remove(ConnectionFactory.class);

			if (connectionFactory instanceof Closeable) {
				StepVerifier.create(((Closeable) connectionFactory).close()).verifyComplete();
			}
		}
	}

	@Override
	public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
			throws ParameterResolutionException {
		return parameterContext.getParameter().getType().isAssignableFrom(Connection.class)
				|| parameterContext.getParameter().getType().isAssignableFrom(ConnectionFactory.class);
	}

	@Override
	public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
			throws ParameterResolutionException {

		ExtensionContext.Store store = extensionContext.getStore(R2DBC);

		if (parameterContext.getParameter().getType().isAssignableFrom(Connection.class)) {
			return getConnection(store);
		}

		if (parameterContext.getParameter().getType().isAssignableFrom(ConnectionFactory.class)) {
			return getConnectionFactory(store);
		}

		throw new ParameterResolutionException("¯\\_(ツ)_/¯");
	}

	private Connection getConnection(ExtensionContext.Store store) {

		Connection connection = store.get(Connection.class, Connection.class);
		if (connection == null) {

			ConnectionFactory connectionFactory = getConnectionFactory(store);
			connection = Mono.from(connectionFactory.create()).block();
			store.put(Connection.class, connection);
		}

		return connection;
	}

	abstract ConnectionFactory getConnectionFactory(ExtensionContext.Store store);
}
