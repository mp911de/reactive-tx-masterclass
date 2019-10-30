package rxtx.plain;

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import rxtx.extension.Neo4jDriverExtension;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnJre;
import org.junit.jupiter.api.condition.JRE;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.driver.Driver;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.reactive.RxStatementRunner;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.summary.SummaryCounters;

@ExtendWith(Neo4jDriverExtension.class)
@DisabledOnJre({ JRE.JAVA_8, JRE.JAVA_9, JRE.JAVA_10 }) // Neo4j 4.0 embedded is JDK 11+
public class Neo4jTransactionTests {

	@BeforeAll
	static void createConstraints(Driver driver) {

		Flux.using(driver::rxSession,
			session -> executeUpdate(session, "CREATE CONSTRAINT ON (person:Person) ASSERT person.name IS UNIQUE"),
			RxSession::close
		).then().as(StepVerifier::create).verifyComplete();
	}

	@BeforeEach
	void clearDatabase(Driver driver) {

		Flux.using(driver::rxSession,
			session -> executeUpdate(session, "MATCH (n) DETACH DELETE n"),
			RxSession::close
		).then().as(StepVerifier::create).verifyComplete();
	}

	@Test
	void autoCommit(Driver driver) {

		Flux<SummaryCounters> createPersonAndMovie =
			Flux.using(driver::rxSession, session -> {
				Flux<SummaryCounters> createPerson = executeUpdate(session, "CREATE (:Person {name: 'Aaron Paul'})");
				Flux<SummaryCounters> createMovie = executeUpdate(session,
					"MATCH (p:Person) WHERE p.name = 'Aaron Paul' WITH p CREATE (p) - [:ACTED_IN {roles: 'Jesse Pinkman'}] -> (:Movie {title: 'El Camino'})");

				return createPerson.thenMany(createMovie);
			}, RxSession::close);

		createPersonAndMovie.then().as(StepVerifier::create)
			.verifyComplete();
	}

	@Test
	void autoCommitWithFailure(Driver driver) {

		Flux<SummaryCounters> createPersonAndMovie =
			Flux.using(driver::rxSession, session -> {
				Flux<SummaryCounters> createPerson = executeUpdate(session, "CREATE (:Person {name: 'Aaron Paul'})");
				Flux<SummaryCounters> createMovie = executeUpdate(session,
					"MATCH (p:Person) WHERE p.name = 'Aaron Paul' WITH p CREATE (p) - [:ACTED_IN {roles: 'Jesse Pinkman'}] -> (:Movie {title: 'El Camino'})");

				return createPerson.thenMany(createMovie);
			}, RxSession::close);

		createPersonAndMovie.then().as(StepVerifier::create)
			.verifyComplete();

		Flux.using(driver::rxSession, session -> executeUpdate(session, "CREATE (:Person {name: 'Aaron Paul'})"),
			RxSession::close)
			.as(StepVerifier::create)
			.verifyErrorMatches(t -> t instanceof ClientException && t.getMessage()
				.endsWith("already exists with label `Person` and property `name` = 'Aaron Paul'"));
	}

	@Test
	void transactionalWithRollback(Driver driver) {

		RxSession session = driver.rxSession();
		Flux.from(session.beginTransaction())
			.flatMap(tx -> {
				Flux<SummaryCounters> createPerson = executeUpdate(tx, "CREATE (:Person {name: 'Aaron Paul'})");
				Flux<SummaryCounters> createMovie = executeUpdate(tx,
					"MATCH (p:Person) WHERE p.name = 'Aaron Paul' WITH p CREATE (p) - [:ACTED_IN {roles: 'Jesse Pinkman'}] -> (:Movie {title: 'El Camino'})");

				createPerson.thenMany(createMovie).then().as(StepVerifier::create).verifyComplete();
				return tx.rollback();
			}).as(StepVerifier::create).verifyComplete();

		Flux.from(session.run("MATCH (p:Person) RETURN p.name AS name").records())
			.map(r -> r.get("name").asString())
			.as(StepVerifier::create)
			.verifyComplete();

		StepVerifier.create(session.close()).verifyComplete();
	}

	private static Flux<SummaryCounters> executeUpdate(RxStatementRunner runner, String cypher) {

		return Flux.defer(() -> runner.run(cypher).summary()).map(ResultSummary::counters);
	}
}
