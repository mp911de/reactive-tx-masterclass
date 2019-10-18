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

import com.mongodb.MongoCommandException;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.Success;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import com.mongodb.MongoWriteException;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;

/**
 * Tests explaining MongoDB transactions using MongoDB API.
 */
@ExtendWith(MongoDBExtension.class)
final class ReactiveMongoTransactionTests {

	@BeforeEach
	void setUp(MongoClient client) {

		MongoDatabase database = client.getDatabase("database");

		Mono.when(database.getCollection("person").drop(), database.getCollection("personEvent").drop())
				.as(StepVerifier::create) //
				.verifyComplete();

		Mono.when(database.createCollection("person"), database.createCollection("personEvent")) //
				.as(StepVerifier::create) //
				.verifyComplete();
	}

	@Test
	void autoCommit(MongoClient client) {

		MongoDatabase database = client.getDatabase("database");
		MongoCollection<Document> person = database.getCollection("person");
		MongoCollection<Document> personEvent = database.getCollection("personEvent");

		StepVerifier.create(person.insertOne(new Document("_id", 1) //
				.append("firstName", "Jesse") //
				.append("lastName", "Pinkman"))) //
				.expectNext(Success.SUCCESS)
				.verifyComplete();

		StepVerifier.create(personEvent.insertOne(new Document("_id", 1) //
				.append("firstName", "Jesse") //
				.append("lastName", "Pinkman").append("ACTION", "CREATED"))) //
				.expectNext(Success.SUCCESS)
				.verifyComplete();
	}

	@Test
	void autoCommitWithFailure(MongoClient client) {

		MongoDatabase database = client.getDatabase("database");
		MongoCollection<Document> person = database.getCollection("person");
		MongoCollection<Document> personEvent = database.getCollection("personEvent");

		StepVerifier.create(person.insertOne(new Document("_id", 1) //
				.append("firstName", "Jesse") //
				.append("lastName", "Pinkman"))) //
				.expectNext(Success.SUCCESS)
				.verifyComplete();

		StepVerifier.create(personEvent.insertOne(new Document("_id", 1) //
				.append("firstName", "Jesse") //
				.append("lastName", "Pinkman").append("ACTION", "CREATED"))) //
				.expectNext(Success.SUCCESS)
				.verifyComplete();

		StepVerifier.create(person.deleteOne(new Document("_id", 1))) //
				.expectNextCount(1) //
				.verifyComplete();

		StepVerifier.create(personEvent.insertOne(new Document("_id", 1) //
				.append("firstName", "Jesse") //
				.append("lastName", "Pinkman").append("ACTION", "CREATED"))) //
				.verifyError(MongoWriteException.class);

		System.out.println("Documents in person");
		Flux.from(person.find()).doOnNext(Documents::print).blockLast();

		System.out.println("Documents in personEvent");
		Flux.from(personEvent.find()).doOnNext(Documents::print).blockLast();
	}

	@Test
	void transactionalWithFailure(MongoClient client) {

		MongoDatabase database = client.getDatabase("database");
		MongoCollection<Document> person = database.getCollection("person");
		MongoCollection<Document> personEvent = database.getCollection("personEvent");

		// BEGIN
		ClientSession session = Mono.from(client.startSession()).block();
		session.startTransaction();

		StepVerifier.create(person.insertOne(session, new Document("_id", 1) //
				.append("firstName", "Jesse") //
				.append("lastName", "Pinkman"))) //
				.expectNext(Success.SUCCESS)
				.verifyComplete();

		StepVerifier.create(personEvent.insertOne(session, new Document("_id", 1) //
				.append("firstName", "Jesse") //
				.append("lastName", "Pinkman").append("ACTION", "CREATED"))) //
				.expectNext(Success.SUCCESS)
				.verifyComplete();

		StepVerifier.create(session.commitTransaction()).verifyComplete();
		session.startTransaction();

		StepVerifier.create(person.deleteOne(session, new Document("_id", 1))) //
				.expectNextCount(1) //
				.verifyComplete();

		StepVerifier.create(personEvent.insertOne(session, new Document("_id", 1) //
				.append("firstName", "Jesse") //
				.append("lastName", "Pinkman").append("ACTION", "DELETED"))) //
				.verifyError(MongoCommandException.class);

		StepVerifier.create(session.abortTransaction()).verifyComplete();

		System.out.println("Documents in person");
		Flux.from(person.find()).doOnNext(Documents::print).blockLast();

		System.out.println("Documents in personEvent");
		Flux.from(personEvent.find()).doOnNext(Documents::print).blockLast();
	}
}
