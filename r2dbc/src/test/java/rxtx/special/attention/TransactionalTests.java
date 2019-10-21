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
package rxtx.special.attention;

import static org.springframework.data.r2dbc.query.Criteria.*;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Result;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import rxtx.extension.R2dbcH2ConnectionExtension;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.annotation.Transient;
import org.springframework.data.r2dbc.connectionfactory.R2dbcTransactionManager;
import org.springframework.data.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;

/**
 * Tests explaining R2DBC transactions using Spring Framework and Spring Data R2DBC.
 */
@ExtendWith(R2dbcH2ConnectionExtension.class)
final class TransactionalTests {

	@BeforeEach
	void setUp(Connection connection) {

		Flux<Integer> drop = executeUpdate(connection, "DROP TABLE IF EXISTS starbucks;");

		drop.then().as(StepVerifier::create).verifyComplete();

		Flux<Integer> create = executeUpdate(connection,
				"CREATE TABLE starbucks (store_number VARCHAR PRIMARY KEY, name VARCHAR(255), address VARCHAR(255), city VARCHAR(255))");
		create.then().as(StepVerifier::create).verifyComplete();
	}

	// Note: Store 74867-97803, exception via handle, 149
	@Test
	void escape(ConnectionFactory connectionFactory) {

		R2dbcTransactionManager transactionManager = new R2dbcTransactionManager(connectionFactory);
		DatabaseClient client = DatabaseClient.create(connectionFactory);
		TransactionalOperator rxtx = TransactionalOperator.create(transactionManager);

		// 74867-97803, handle, 149
		Flux<Map<String, Object>> inTransaction = rxtx.execute(status -> {
			return starbucksRecords().concatMap(it -> {

				String name = it[0];
				String storeNumber = it[2];
				String address = it[5];
				String city = it[9];

				return client.insert().into("starbucks") //
						.value("store_number", storeNumber) //
						.value("name", name) //
						.value("address", address) //
						.value("city", city) //
						.then();

			}).thenMany(client.select().from("starbucks").project("store_number", "name", "address").fetch().all());
		});

		inTransaction.as(StepVerifier::create).expectNextCount(1000).verifyComplete();

		client.execute("SELECT COUNT(*) FROM starbucks").fetch().all().doOnNext(System.out::println).then().block();
	}

	// Note: Enable pooling, run in tx
	@Test
	void nPlusOne(ConnectionFactory connectionFactory) {

		R2dbcTransactionManager transactionManager = new R2dbcTransactionManager(connectionFactory);
		DatabaseClient client = DatabaseClient.create(connectionFactory);
		TransactionalOperator rxtx = TransactionalOperator.create(transactionManager);

		starbucksRecords().concatMap(it -> {

			String name = it[0];
			String storeNumber = it[2];
			String address = it[5];
			String city = it[9];

			return client.insert().into("starbucks") //
					.value("store_number", storeNumber) //
					.value("name", name) //
					.value("address", address) //
					.value("city", city) //
					.then();

		}).then().as(StepVerifier::create).verifyComplete();

		client.select().from(Starbucks.class).fetch().all().flatMap(it -> {

			return client.select().from(Starbucks.class)
					.matching(where("city").is(it.getCity()).and("store_number").not(it.getStoreNumber())).fetch().all()
					.collectList().map(shopsInTheSameCity -> {

						it.setShopsInTheSameCity(shopsInTheSameCity);
						return it;

					});

		}).filter(it -> !it.shopsInTheSameCity.isEmpty()).doOnNext(System.out::println).as(StepVerifier::create)
				.expectNextCount(924).verifyComplete();
	}

	private Flux<Integer> executeUpdate(Connection connection, String sql) {
		return Flux.from(connection.createStatement(sql).execute()).flatMap(Result::getRowsUpdated);
	}

	static class Starbucks {

		String storeNumber;
		String name;
		String address;
		String city;

		@Transient List<Starbucks> shopsInTheSameCity;

		public String getStoreNumber() {
			return this.storeNumber;
		}

		public void setStoreNumber(String storeNumber) {
			this.storeNumber = storeNumber;
		}

		public String getName() {
			return this.name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getAddress() {
			return this.address;
		}

		public void setAddress(String address) {
			this.address = address;
		}

		public String getCity() {
			return this.city;
		}

		public void setCity(String city) {
			this.city = city;
		}

		public List<Starbucks> getShopsInTheSameCity() {
			return this.shopsInTheSameCity;
		}

		public void setShopsInTheSameCity(List<Starbucks> shopsInTheSameCity) {
			this.shopsInTheSameCity = shopsInTheSameCity;
		}

		@Override
		public String toString() {
			return "Starbucks{" + "storeNumber='" + storeNumber + '\'' + ", name='" + name + '\'' + ", address='" + address
					+ '\'' + ", city='" + city + '\'' + ", shopsInTheSameCity=" + shopsInTheSameCity + '}';
		}
	}

	static Flux<String[]> starbucksRecords() {

		BufferedReader br;
		try {
			br = new BufferedReader(
					new InputStreamReader(new ClassPathResource("all-starbucks-locations-in-the-world.csv").getInputStream()));
			br.readLine();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		return Flux.create(fluxSink -> {

			fluxSink.onCancel(() -> {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			});

			fluxSink.onRequest(value -> {

				try {
					for (int i = 0; i < value; i++) {
						String s = br.readLine();
						if (s == null) {
							fluxSink.complete();
							return;
						}
						fluxSink.next(s.split(";"));
					}
				} catch (IOException e) {
					fluxSink.error(e);
				}
			});
		});
	}

}
