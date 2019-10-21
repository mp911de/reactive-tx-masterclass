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
package rxtx.plain;

import static org.junit.jupiter.api.Assertions.*;

import java.util.function.Consumer;

import com.mongodb.MongoCommandException;
import com.mongodb.client.ClientSession;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import rxtx.Documents;
import rxtx.extension.MongoDBExtension;

/**
 * Tests explaining MongoDB transactions using MongoDB API.
 */
@ExtendWith(MongoDBExtension.class)
final class MongoTransactionTests {

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

		person.insertOne(new Document("_id", 1).append("firstName", "Jesse").append("lastName", "Pinkman"));
		personEvent.insertOne(
				new Document("_id", 1).append("firstName", "Jesse").append("lastName", "Pinkman").append("ACTION", "CREATED"));
	}

	@Test
	void autoCommitWithFailure(MongoClient client) {

		MongoDatabase database = client.getDatabase("database");
		MongoCollection<Document> person = database.getCollection("person");
		MongoCollection<Document> personEvent = database.getCollection("personEvent");

		person.insertOne(new Document("_id", 1).append("firstName", "Jesse").append("lastName", "Pinkman"));
		personEvent.insertOne(
				new Document("_id", 1).append("firstName", "Jesse").append("lastName", "Pinkman").append("ACTION", "CREATED"));

		person.deleteOne(new Document("_id", 1));

		try {
			personEvent.insertOne(new Document("_id", 1).append("firstName", "Jesse").append("lastName", "Pinkman")
					.append("ACTION", "DELETED"));
			fail("Missing MongoWriteException");
		} catch (MongoWriteException e) {
			// expected exception
		}

		System.out.println("Documents in person");
		person.find().forEach((Consumer<Document>) Documents::print);

		System.out.println("Documents in personEvent");
		personEvent.find().forEach((Consumer<Document>) Documents::print);
	}

	@Test
	void transactionalWithRollback(MongoClient client) {

		MongoDatabase database = client.getDatabase("database");
		MongoCollection<Document> person = database.getCollection("person");
		MongoCollection<Document> personEvent = database.getCollection("personEvent");

		// BEGIN
		ClientSession session = client.startSession();
		session.startTransaction();

		person.insertOne(session, new Document("_id", 1).append("firstName", "Jesse").append("lastName", "Pinkman"));
		personEvent.insertOne(session,
				new Document("_id", 1).append("firstName", "Jesse").append("lastName", "Pinkman").append("ACTION", "CREATED"));
		session.commitTransaction();
		session.startTransaction();

		person.deleteOne(session, new Document("_id", 1));

		try {
			personEvent.insertOne(session, new Document("_id", 1).append("firstName", "Jesse").append("lastName", "Pinkman")
					.append("ACTION", "DELETED"));
			fail("Missing MongoWriteException");
		} catch (MongoCommandException e) {
			// expected exception
			session.abortTransaction();
			session.close();
		}

		System.out.println("Documents in person");
		person.find().forEach((Consumer<Document>) Documents::print);

		System.out.println("Documents in personEvent");
		personEvent.find().forEach((Consumer<Document>) Documents::print);
	}
}
