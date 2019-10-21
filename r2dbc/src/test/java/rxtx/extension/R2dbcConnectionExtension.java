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

import io.r2dbc.h2.CloseableConnectionFactory;
import io.r2dbc.h2.H2ConnectionFactory;
import io.r2dbc.spi.Connection;
import reactor.test.StepVerifier;

import java.util.UUID;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

/**
 * Extension providing a R2DBC {@link Connection}.
 */
public class R2dbcConnectionExtension implements AfterEachCallback, ParameterResolver {

	private static final ExtensionContext.Namespace R2DBC = ExtensionContext.Namespace.create("R2DBC");

	@Override
	public void afterEach(ExtensionContext context) {

		ExtensionContext.Store store = context.getStore(R2DBC);

		Connection connection = store.get(Connection.class, Connection.class);
		if (connection != null) {
			store.remove(Connection.class);
			StepVerifier.create(connection.close()).verifyComplete();
		}

		CloseableConnectionFactory connectionFactory = store.get(CloseableConnectionFactory.class,
				CloseableConnectionFactory.class);
		if (connectionFactory != null) {
			store.remove(CloseableConnectionFactory.class);
			StepVerifier.create(connectionFactory.close()).verifyComplete();
		}
	}

	@Override
	public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
			throws ParameterResolutionException {
		return parameterContext.getParameter().getType().isAssignableFrom(Connection.class);
	}

	@Override
	public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
			throws ParameterResolutionException {

		ExtensionContext.Store store = extensionContext.getStore(R2DBC);

		if (parameterContext.getParameter().getType().isAssignableFrom(Connection.class)) {
			return getConnection(store);
		}

		throw new ParameterResolutionException("¯\\_(ツ)_/¯");
	}

	private Connection getConnection(ExtensionContext.Store store) {

		Connection connection = store.get(Connection.class, Connection.class);
		if (connection == null) {

			CloseableConnectionFactory connectionFactory = getConnectionFactory(store);
			connection = connectionFactory.create().block();
			store.put(Connection.class, connection);
		}

		return connection;
	}

	private CloseableConnectionFactory getConnectionFactory(ExtensionContext.Store store) {

		CloseableConnectionFactory connectionFactory = store.get(CloseableConnectionFactory.class,
				CloseableConnectionFactory.class);
		if (connectionFactory == null) {
			connectionFactory = H2ConnectionFactory.inMemory(UUID.randomUUID().toString());
			store.put(CloseableConnectionFactory.class, connectionFactory);
		}

		return connectionFactory;
	}
}
