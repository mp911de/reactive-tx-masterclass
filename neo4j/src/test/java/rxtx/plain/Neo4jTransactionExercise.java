package rxtx.plain;

import static rxtx.plain.Neo4jTransactionTests.*;

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import rxtx.extension.Neo4jDriverExtension;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.driver.Driver;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.reactive.RxTransaction;
import org.neo4j.driver.reactive.RxTransactionWork;
import org.neo4j.driver.summary.SummaryCounters;
import org.reactivestreams.Publisher;

@ExtendWith(Neo4jDriverExtension.class)
@Disabled // Disabled as they will fill
public class Neo4jTransactionExercise {

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

		Flux<SummaryCounters> createPerson = Flux.empty();
		Flux<SummaryCounters> createMovie = Flux.empty();
		Flux<SummaryCounters> createPersonAndMovie = createPerson
			.thenMany(createMovie);

		createPersonAndMovie.as(StepVerifier::create)
			.expectNextMatches(c -> c.nodesCreated() == 1 && c.relationshipsCreated() == 1)
			.verifyComplete();
	}

	@Test
	void transactionalWithRollback(Driver driver) {
		Flux.using(driver::rxSession, tx -> {
			Flux<SummaryCounters> createPerson = executeUpdate(tx, "CREATE (:Person {name: 'Aaron Paul'})");
			Flux<SummaryCounters> createMovie = executeUpdate(tx,
				"MATCH (p:Person) WHERE p.name = 'Aaron Paul' WITH p CREATE (p) - [:ACTED_IN {roles: 'Jesse Pinkman'}] -> (:Movie {title: 'El Camino'})");

			return createPerson.thenMany(createMovie);
		}, RxSession::close).as(StepVerifier::create).verifyComplete();

		RxSession session = driver.rxSession();
		Flux.from(session.run("MATCH (p:Person) RETURN p.name AS name").records())
			.map(r -> r.get("name").asString())
			.as(StepVerifier::create)
			.verifyComplete();

		StepVerifier.create(session.close()).verifyComplete();
	}

	@Test
	void transactionalWithRollbackBetterVariant(Driver driver) {

		RxSession session = driver.rxSession();

		Flux.usingWhen(session.beginTransaction(),
			tx -> {
				Flux<SummaryCounters> createPerson = Flux.empty();
				Flux<SummaryCounters> createMovie = Flux.empty();

				return Flux.empty();
			},
			RxTransaction::commit,
			(tx, exception) -> tx.rollback(),
			RxTransaction::commit
		).as(StepVerifier::create).verifyErrorMessage("Throwing a business error");

		Flux.from(session.run("MATCH (p:Person) RETURN p.name AS name").records())
			.map(r -> r.get("name").asString())
			.as(StepVerifier::create)
			.verifyComplete();

		StepVerifier.create(session.close()).verifyComplete();
	}

	@Test
	void transactionalWithRollbackBetterVariant2(Driver driver) {

		RxTransactionWork<Publisher<Integer>> txFunction = tx -> Flux.empty();

		Flux.using(driver::rxSession,
			session -> session.writeTransaction(txFunction)
			, RxSession::close)
			.as(StepVerifier::create).verifyErrorMessage("Throwing a business error");

		RxSession session = driver.rxSession();

		Flux.from(session.run("MATCH (p:Person) RETURN p.name AS name").records())
			.map(r -> r.get("name").asString())
			.as(StepVerifier::create)
			.verifyComplete();

		StepVerifier.create(session.close()).verifyComplete();
	}
}
