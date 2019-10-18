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

import java.util.function.Consumer;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * Tests explaining MongoDB transactions using MongoDB API.
 */
@ExtendWith(MongoDBExtension.class)
final class MongoTransactionExcercise {

	@BeforeEach
	void setUp(MongoClient client) {

		MongoDatabase database = client.getDatabase("database");

		database.getCollection("person").drop();
		database.getCollection("personEvent").drop();

		database.createCollection("person");
		database.createCollection("personEvent");
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

		// insert Person, Person Event

		// delete Person

		// Insert PersonEvent

		System.out.println("Documents in person");
		person.find().forEach((Consumer<Document>) Documents::print);

		System.out.println("Documents in personEvent");
		personEvent.find().forEach((Consumer<Document>) Documents::print);
	}

	@Test
	void transactionalWithFailure(MongoClient client) {

		MongoDatabase database = client.getDatabase("database");
		MongoCollection<Document> person = database.getCollection("person");
		MongoCollection<Document> personEvent = database.getCollection("personEvent");

		// BEGIN
		// insert Person, Person Event

		// delete Person

		// Insert PersonEvent

		System.out.println("Documents in person");
		person.find().forEach((Consumer<Document>) Documents::print);

		System.out.println("Documents in personEvent");
		personEvent.find().forEach((Consumer<Document>) Documents::print);
	}
}
