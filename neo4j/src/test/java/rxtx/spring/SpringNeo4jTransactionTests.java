package rxtx.spring;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnJre;
import org.junit.jupiter.api.condition.JRE;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Logging;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.harness.internal.TestNeo4jBuilders;
import org.neo4j.harness.junit.extension.Neo4j;
import org.neo4j.springframework.data.config.AbstractReactiveNeo4jConfig;
import org.neo4j.springframework.data.core.ReactiveNeo4jClient;
import org.neo4j.springframework.data.repository.config.EnableReactiveNeo4jRepositories;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.reactive.TransactionalOperator;

@ExtendWith(SpringExtension.class)
@DisabledOnJre({ JRE.JAVA_8, JRE.JAVA_9, JRE.JAVA_10 }) // Neo4j 4.0 embedded is JDK 11+
class SpringNeo4jTransactionTests {

	@BeforeAll
	static void createConstraints(@Autowired Driver driver) {

		Flux.using(driver::rxSession,
			session -> session.run("CREATE CONSTRAINT ON (person:Person) ASSERT person.name IS UNIQUE").summary(),
			RxSession::close
		).then().as(StepVerifier::create).verifyComplete();
	}

	@BeforeEach
	void clearDatabase(@Autowired Driver driver) {

		Flux.using(driver::rxSession,
			session -> session.run("MATCH (n) DETACH DELETE n").summary(),
			RxSession::close
		).then().as(StepVerifier::create).verifyComplete();
	}

	@Test
	void autoCommit(@Autowired ReactiveNeo4jClient neo4jClient) {

		Mono<ResultSummary> createPerson = neo4jClient
			.query("CREATE (:Person {name: 'Aaron Paul'})").run();
		Mono<ResultSummary> createMovie = neo4jClient
			.query(
				"MATCH (p:Person) WHERE p.name = 'Aaron Paul' WITH p CREATE (p) - [:ACTED_IN {roles: 'Jesse Pinkman'}] -> (:Movie {title: 'El Camino'})")
			.run();

		createPerson.then(createMovie).then().as(StepVerifier::create).verifyComplete();
	}

	@Test
	void autoCommitWithFailure(@Autowired ReactiveNeo4jClient neo4jClient) {

		Mono<ResultSummary> createPerson = neo4jClient
			.query("CREATE (:Person {name: 'Aaron Paul'})").run();
		Mono<ResultSummary> createMovie = neo4jClient
			.query(
				"MATCH (p:Person) WHERE p.name = 'Aaron Paul' WITH p CREATE (p) - [:ACTED_IN {roles: 'Jesse Pinkman'}] -> (:Movie {title: 'El Camino'})")
			.run();

		createPerson.then(createMovie).then().as(StepVerifier::create).verifyComplete();

		neo4jClient
			.query("CREATE (:Person {name: 'Aaron Paul'})").run()
			.as(StepVerifier::create)
			.verifyErrorMatches(ex -> {
				Throwable cause = ex.getCause(); // See https://github.com/neo4j/sdn-rx/issues/91
				return cause instanceof ClientException && cause.getMessage()
					.endsWith("already exists with label `Person` and property `name` = 'Aaron Paul'");
			});
	}

	@Test
	void programmaticTransactionalWithRollback(
		@Autowired Driver driver,
		@Autowired ReactiveNeo4jClient neo4jClient,
		@Autowired TransactionalOperator rxtx) {

		// Create some data
		Mono<ResultSummary> createPerson = neo4jClient
			.query("CREATE (:Person {name: 'Aaron Paul'})").run();
		Mono<ResultSummary> createMovie = neo4jClient
			.query(
				"MATCH (p:Person) WHERE p.name = 'Aaron Paul' WITH p CREATE (p) - [:ACTED_IN {roles: 'Jesse Pinkman'}] -> (:Movie {title: 'El Camino'})")
			.run();

		createPerson.then(createMovie).as(rxtx::transactional).then().as(StepVerifier::create).verifyComplete();

		// Aaron is a duplicate
		Mono<ResultSummary> createPeople =
			neo4jClient
				.query("CREATE (:Person {name: 'Bryan Cranston'})").run()
				.then(
					neo4jClient
						.query("CREATE (:Person {name: 'Aaron Paul'})").run());

		createPeople.as(rxtx::transactional).then().as(StepVerifier::create).verifyError();

		Flux.using(driver::rxSession, session -> session.run("MATCH (p:Person) RETURN p.name AS name").records(),
			RxSession::close)
			.map(r -> r.get("name").asString())
			.as(StepVerifier::create)
			.expectNext("Aaron Paul")
			.verifyComplete();
	}

	@Test
	void atTransactionalWithRollback(@Autowired Driver driver, @Autowired TransactionalService transactionalService) {

		transactionalService.insert().as(StepVerifier::create).verifyComplete();

		transactionalService.insertAgain().as(StepVerifier::create).verifyError();

		Flux.using(driver::rxSession, session -> session.run("MATCH (p:Person) RETURN p.name AS name").records(),
			RxSession::close)
			.map(r -> r.get("name").asString())
			.as(StepVerifier::create)
			.expectNext("Aaron Paul")
			.verifyComplete();
	}

	@Configuration
	@EnableReactiveNeo4jRepositories
	@EnableTransactionManagement
	static class SDNRXConfig extends AbstractReactiveNeo4jConfig {

		@Bean
		Neo4j neo4j() {
			return TestNeo4jBuilders.newInProcessBuilder().build();
		}

		@Bean
		TransactionalOperator rxtx(ReactiveTransactionManager transactionManager) {
			return TransactionalOperator.create(transactionManager);
		}

		@Bean
		public Driver driver() {

			Config config = Config.builder().withLogging(Logging.slf4j()).build();
			return GraphDatabase.driver(neo4j().boltURI(), config);
		}

		@Bean
		TransactionalService transactionalService(ReactiveNeo4jClient databaseClient) {
			return new TransactionalService(databaseClient);
		}
	}

	@Transactional
	public static class TransactionalService {

		private final ReactiveNeo4jClient databaseClient;

		public TransactionalService(ReactiveNeo4jClient databaseClient) {
			this.databaseClient = databaseClient;
		}

		public Mono<Void> insert() {

			Mono<ResultSummary> createPerson = databaseClient
				.query("CREATE (:Person {name: 'Aaron Paul'})").run();
			Mono<ResultSummary> createMovie = databaseClient
				.query(
					"MATCH (p:Person) WHERE p.name = 'Aaron Paul' WITH p CREATE (p) - [:ACTED_IN {roles: 'Jesse Pinkman'}] -> (:Movie {title: 'El Camino'})")
				.run();

			return createPerson.then(createMovie).then();
		}

		public Mono<Void> insertAgain() {
			return databaseClient
				.query("CREATE (:Person {name: 'Bryan Cranston'})").run()
				.then(
					databaseClient
						.query("CREATE (:Person {name: 'Aaron Paul'})").run()).then();
		}
	}
}
