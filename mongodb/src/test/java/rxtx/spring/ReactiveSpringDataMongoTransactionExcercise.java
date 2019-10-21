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

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
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
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.reactive.TransactionalOperator;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;

/**
 * Tests explaining MongoDB transactions using Spring Framework and Spring Data MongoDB.
 */
@ExtendWith(MongoDBExtension.class)
@SpringBootTest(properties = "spring.data.mongodb.uri=mongodb://localhost/database?replicaSet=rs0&retryWrites=false",
		classes = ReactiveSpringDataMongoTransactionExcercise.TestConfiguration.class)
final class ReactiveSpringDataMongoTransactionExcercise {

	@BeforeEach
	void setUp(MongoClient client) {

		MongoDatabase database = client.getDatabase("database");

		database.getCollection("person").drop();
		database.getCollection("personEvent").drop();

		database.createCollection("person");
		database.createCollection("personEvent");
	}

	@Test
	void autoCommit(@Autowired ReactiveMongoOperations operations) {

		Mono<Document> insertPerson = Mono.empty();
		Mono<Document> insertEvent = Mono.empty();

		insertPerson.then(insertEvent).then().as(StepVerifier::create).verifyComplete();
	}

	@Test
	void autoCommitWithFailure(@Autowired ReactiveMongoOperations operations) {

		Mono<Document> insertPerson = Mono.empty();
		Mono<Document> insertEvent = Mono.empty();

		Mono<DeleteResult> removePerson = Mono.empty();
		Mono<Document> insertDeleteEvent = Mono.empty();

		System.out.println("Documents in person");
		operations.find(new Query(), Document.class, "person").toStream().forEach(Documents::print);

		System.out.println("Documents in personEvent");
		operations.find(new Query(), Document.class, "personEvent").toStream().forEach(Documents::print);
	}

	@Test
	void programmaticTransactionalWithRollback(@Autowired ReactiveMongoOperations operations,
			@Autowired TransactionalOperator rxtx) {

		Mono<Document> insertPerson = Mono.empty();
		Mono<Document> insertEvent = Mono.empty();

		Mono<DeleteResult> removePerson = Mono.empty();
		Mono<Document> insertDeleteEvent = Mono.empty();

		System.out.println("Documents in person");
		operations.find(new Query(), Document.class, "person").toStream().forEach(Documents::print);

		System.out.println("Documents in personEvent");
		operations.find(new Query(), Document.class, "personEvent").toStream().forEach(Documents::print);
	}

	@Test
	void atTransactionalWithRollback(@Autowired ReactiveMongoOperations operations,
			@Autowired TransactionalService transactionalService) {

		System.out.println("Documents in person");
		operations.find(new Query(), Document.class, "person").toStream().forEach(Documents::print);

		System.out.println("Documents in personEvent");
		operations.find(new Query(), Document.class, "personEvent").toStream().forEach(Documents::print);
	}

	@SpringBootApplication(exclude = EmbeddedMongoAutoConfiguration.class)
	static class TestConfiguration {

		@Bean
		ReactiveMongoTransactionManager mongoTransactionManager(ReactiveMongoDatabaseFactory dbFactory) {
			return new ReactiveMongoTransactionManager(dbFactory);
		}

		@Bean
		TransactionalService transactionalService(ReactiveMongoOperations operations) {
			return new TransactionalService(operations);
		}

	}

	@Transactional
	public static class TransactionalService {

		private final ReactiveMongoOperations operations;

		public TransactionalService(ReactiveMongoOperations operations) {
			this.operations = operations;
		}

		public Mono<Void> insert() {

			Mono<Document> insertPerson = operations
					.insert(new Document("_id", 1).append("firstName", "Jesse").append("lastName", "Pinkman"), "person");
			Mono<Document> insertEvent = operations.insert(
					new Document("_id", 1).append("firstName", "Jesse").append("lastName", "Pinkman").append("ACTION", "CREATED"),
					"personEvent");

			return insertPerson.then(insertEvent).then();
		}

		public Mono<Void> delete() {

			Mono<DeleteResult> removePerson = operations.remove(query(where("_id").is(1)), "person");
			Mono<Document> insertEvent = operations.insert(
					new Document("_id", 1).append("firstName", "Jesse").append("lastName", "Pinkman").append("ACTION", "DELETED"),
					"personEvent");

			return removePerson.then(insertEvent).then();
		}

	}

}
