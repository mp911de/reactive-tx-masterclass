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

import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Extension providing a R2DBC {@link Connection} to a H2 in-memory database.
 */
public class R2dbcH2ConnectionExtension extends AbstractR2dbcConnectionExtension {

	CloseableConnectionFactory getConnectionFactory(ExtensionContext.Store store) {

		CloseableConnectionFactory connectionFactory = store.get(CloseableConnectionFactory.class,
				CloseableConnectionFactory.class);
		if (connectionFactory == null) {
			connectionFactory = H2ConnectionFactory.inMemory("R2dbcConnectionExtension");
			store.put(CloseableConnectionFactory.class, connectionFactory);
		}

		return connectionFactory;
	}
}
