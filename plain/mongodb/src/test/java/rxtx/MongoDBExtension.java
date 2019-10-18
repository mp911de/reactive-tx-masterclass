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

import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongoCmdOptionsBuilder;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.config.RuntimeConfigBuilder;
import de.flapdoodle.embed.mongo.config.Storage;
import de.flapdoodle.embed.mongo.distribution.Feature;
import de.flapdoodle.embed.mongo.distribution.IFeatureAwareVersion;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.mongo.distribution.Versions;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.config.io.ProcessOutput;
import de.flapdoodle.embed.process.distribution.GenericVersion;
import de.flapdoodle.embed.process.io.Processors;
import de.flapdoodle.embed.process.runtime.Network;

import java.sql.Connection;
import java.sql.Statement;
import java.util.EnumSet;
import java.util.List;

import org.bson.Document;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import com.mongodb.MongoSocketReadException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

/**
 * Extension providing a JDBC {@link Connection}.
 */
final class MongoDBExtension implements BeforeAllCallback, AfterEachCallback, AfterAllCallback, ParameterResolver {

	private static final ExtensionContext.Namespace MONGO = ExtensionContext.Namespace.create("MONGO");

	@Override
	public void beforeAll(ExtensionContext context) throws Exception {

		ExtensionContext.Store store = context.getStore(MONGO);

		EnumSet<Feature> features = Version.Main.PRODUCTION.getFeatures();
		IFeatureAwareVersion version = Versions.withFeatures(new GenericVersion("4.0.2"), features);

		IMongodConfig config = new MongodConfigBuilder() //
				.cmdOptions(new MongoCmdOptionsBuilder().useNoJournal(false).build()) //
				.version(version) //
				.net(new Net(27017, Network.localhostIsIPv6())) //
				.replication(new Storage(null, "rs0", 10)) //
				.build();

		IRuntimeConfig runtimeConfig = new RuntimeConfigBuilder().defaults(Command.MongoD)
				.processOutput(new ProcessOutput(Processors.silent(), Processors.console(), Processors.silent()))
				.build();

		MongodStarter starter = MongodStarter.getInstance(runtimeConfig);
		MongodExecutable executable = starter.prepare(config);

		store.put(MongodProcess.class, executable.start());

		MongoClient mongoClient = MongoClients.create();
		MongoDatabase database = mongoClient.getDatabase("admin");
		database.runCommand(new Document("replSetInitiate", new Document()));

		Document replSetGetStatus;

		// Check replica set status before to proceed
		do {

			System.out.println("Waiting for 3 seconds...");
			Thread.sleep(3000);
			replSetGetStatus = database.runCommand(new Document("replSetGetStatus", 1));
		} while (!isReplicaSetStarted(replSetGetStatus));

		mongoClient.close();

		mongoClient = MongoClients.create("mongodb://localhost:27017/?replicaSet=rs0&retryWrites=false");
		store.put(MongoClient.class, mongoClient);
	}

	private boolean isReplicaSetStarted(Document replSetGetStatus) {
		if (replSetGetStatus.get("members") == null) {
			return false;
		}

		List<Document> members = (List) replSetGetStatus.get("members");
		for (Document m : members) {
			int state = m.getInteger("state");
			// 1 - PRIMARY, 2 - SECONDARY, 7 - ARBITER
			if (state != 1 && state != 2 && state != 7) {
				return false;
			}
		}
		return true;
	}

	@Override
	public void afterEach(ExtensionContext context) throws Exception {

		ExtensionContext.Store store = context.getStore(MONGO);
		Statement statement = store.get(Statement.class, Statement.class);
		if (statement != null) {
			store.remove(Statement.class);
			statement.close();
		}

		Connection connection = store.get(Connection.class, Connection.class);
		if (connection != null) {
			store.remove(Connection.class);
			connection.close();
		}
	}

	@Override
	public void afterAll(ExtensionContext context) {

		ExtensionContext.Store store = context.getStore(MONGO);
		MongodProcess process = store.get(MongodProcess.class, MongodProcess.class);
		MongoClient mongoClient = store.get(MongoClient.class, MongoClient.class);

		if (mongoClient != null) {

			MongoDatabase database = mongoClient.getDatabase("admin");

			try {
				database.runCommand(new Document("shutdown", 1).append("force", true).append("timeoutSecs", 1));
			} catch (MongoSocketReadException e) {
				// All good
			}
			mongoClient.close();
		}

		if (process != null) {

			process.stop();
			store.remove(MongodProcess.class);
		}
	}

	@Override
	public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
			throws ParameterResolutionException {
		return parameterContext.getParameter().getType().isAssignableFrom(MongoClient.class);
	}

	@Override
	public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
			throws ParameterResolutionException {

		ExtensionContext.Store store = extensionContext.getStore(MONGO);

		if (parameterContext.getParameter().getType().isAssignableFrom(MongoClient.class)) {
			return store.get(MongoClient.class);
		}

		throw new ParameterResolutionException("¯\\_(ツ)_/¯");
	}
}
