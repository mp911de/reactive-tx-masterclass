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

import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import rxtx.Documents;
import rxtx.extension.MongoDBExtension;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.mongo.embedded.EmbeddedMongoAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionOperations;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;

/**
 * Tests explaining MongoDB transactions using Spring Framework and Spring Data MongoDB.
 */
@ExtendWith(MongoDBExtension.class)
@SpringBootTest(properties = "spring.data.mongodb.uri=mongodb://localhost/database?replicaSet=rs0&retryWrites=false",
		classes = SpringDataMongoTransactionExcercise.TestConfiguration.class)
final class SpringDataMongoTransactionExcercise {

	@BeforeEach
	void setUp(MongoClient client) {

		MongoDatabase database = client.getDatabase("database");

		database.getCollection("person").drop();
		database.getCollection("personEvent").drop();

		database.createCollection("person");
		database.createCollection("personEvent");
	}

	@Test
	void autoCommit(@Autowired MongoOperations operations) {

	}

	@Test
	void autoCommitWithFailure(@Autowired MongoOperations operations) {

		System.out.println("Documents in person");
		operations.stream(new Query(), Document.class, "person").forEachRemaining(Documents::print);

		System.out.println("Documents in personEvent");
		operations.stream(new Query(), Document.class, "personEvent").forEachRemaining(Documents::print);
	}

	@Test
	void programmaticTransactionalWithFailure(@Autowired MongoOperations operations,
			@Autowired TransactionOperations tx) {

		System.out.println("Documents in person");
		operations.stream(new Query(), Document.class, "person").forEachRemaining(Documents::print);

		System.out.println("Documents in personEvent");
		operations.stream(new Query(), Document.class, "personEvent").forEachRemaining(Documents::print);
	}

	@Test
	void atTransactionalWithFailure(@Autowired MongoOperations operations,
			@Autowired TransactionalService transactionalService) {

		System.out.println("Documents in person");
		operations.stream(new Query(), Document.class, "person").forEachRemaining(Documents::print);

		System.out.println("Documents in personEvent");
		operations.stream(new Query(), Document.class, "personEvent").forEachRemaining(Documents::print);
	}

	@SpringBootApplication(exclude = EmbeddedMongoAutoConfiguration.class)
	static class TestConfiguration {

		@Bean
		MongoTransactionManager mongoTransactionManager(MongoDbFactory dbFactory) {
			return new MongoTransactionManager(dbFactory);
		}

		@Bean
		TransactionalService transactionalService(MongoOperations operations) {
			return new TransactionalService(operations);
		}

	}

	@Transactional
	public static class TransactionalService {

		private final MongoOperations operations;

		public TransactionalService(MongoOperations operations) {
			this.operations = operations;
		}

		public void insert() {
			operations.insert(new Document("_id", 1).append("firstName", "Jesse").append("lastName", "Pinkman"), "person");
			operations.insert(
					new Document("_id", 1).append("firstName", "Jesse").append("lastName", "Pinkman").append("ACTION", "CREATED"),
					"personEvent");
		}

		public void delete() {
			operations.remove(query(where("_id").is(1)), "person");
			operations.insert(
					new Document("_id", 1).append("firstName", "Jesse").append("lastName", "Pinkman").append("ACTION", "DELETED"),
					"personEvent");
		}

	}

}
