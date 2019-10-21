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

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;

import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Extension providing a R2DBC {@link Connection} to a local Postgres Database.
 */
public class R2dbcPostgresConnectionExtension extends AbstractR2dbcConnectionExtension {

	ConnectionFactory getConnectionFactory(ExtensionContext.Store store) {

		ConnectionFactory connectionFactory = store.get(ConnectionFactory.class, ConnectionFactory.class);
		if (connectionFactory == null) {

			connectionFactory = ConnectionFactories.get("r2dbc:postgres://postgres:postgres@localhost/postgres");
			store.put(ConnectionFactory.class, connectionFactory);
		}

		return connectionFactory;
	}
}
