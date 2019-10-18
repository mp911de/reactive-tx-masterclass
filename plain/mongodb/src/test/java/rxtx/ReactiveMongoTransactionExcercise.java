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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;

/**
 * Tests explaining MongoDB transactions using MongoDB API.
 */
@ExtendWith(MongoDBExtension.class)
final class ReactiveMongoTransactionExcercise {

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

		// insert
	}

	@Test
	void autoCommitWithFailure(MongoClient client) {

		MongoDatabase database = client.getDatabase("database");
		MongoCollection<Document> person = database.getCollection("person");
		MongoCollection<Document> personEvent = database.getCollection("personEvent");

		// insert, delete

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
		// insert, delete transactional

		System.out.println("Documents in person");
		Flux.from(person.find()).doOnNext(Documents::print).blockLast();

		System.out.println("Documents in personEvent");
		Flux.from(personEvent.find()).doOnNext(Documents::print).blockLast();
	}
}
