package rxtx.plain;

import static org.neo4j.driver.Values.*;

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import rxtx.extension.Neo4jDriverExtension;

import java.util.function.Function;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnJre;
import org.junit.jupiter.api.condition.JRE;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.reactive.RxTransaction;
import org.neo4j.driver.reactive.RxTransactionWork;
import org.reactivestreams.Publisher;

/**
 * A suite of tests explaining the different options Neo4j driver offers.
 */
@DisabledOnJre({ JRE.JAVA_8, JRE.JAVA_9, JRE.JAVA_10 }) // Neo4j 4.0 embedded is JDK 11+
class Neo4jTransactionTypesTests {

	private static final String NAME = "Linda Hamilton";

	@Nested
	@ExtendWith(Neo4jDriverExtension.class)
	class ImperativeTransactions {

		private final Driver driver;

		public ImperativeTransactions(Driver driver) {
			this.driver = driver;
		}

		@Test
		void implicit() { // Aka "Auto-Commit"

			try (Session session = driver.session()) {
				long personId = session
					.run("CREATE (a:Person {name: $name}) RETURN id(a) as id", parameters("name", NAME))
					.single().get("id").asLong();
			}
		}

		/**
		 * Delegates things to the driver:
		 * Transaction functions are able to handle connection problems and transient
		 * errors using an automatic retry mechanism. Can deal with leader relections inside
		 * a cluster and similar transient errors.
		 */
		@Test
		void txFunctions() {

			try (Session session = driver.session()) {
				long personId = session.writeTransaction(
					tx -> tx.run("CREATE (a:Person {name: $name}) RETURN id(a) as id", parameters("name", NAME))
						.single().get("id").asLong());
			}
		}

		/**
		 * Most useful for framework code, as it gives the lifecycle of the transaction
		 * away from the driver into the callers hands.
		 */
		@Test
		void explicit() {

			try (Session session = driver.session()) {

				Transaction tx = session.beginTransaction(TransactionConfig.builder().build());

				long personId =
					tx.run("CREATE (a:Person {name: $name}) RETURN id(a) as id", parameters("name", NAME))
						.single().get("id").asLong();

				tx.commit();
			}
		}
	}

	@Nested
	@ExtendWith(Neo4jDriverExtension.class)
	class ReactiveTransactions {

		private final Driver driver;

		public ReactiveTransactions(Driver driver) {
			this.driver = driver;
		}

		@Test
		void implicit() {

			Flux<Long> personCreation = Flux.using(driver::rxSession,
				session -> Flux.from(session
					// Same signature as imperative
					.run("CREATE (a:Person {name: $name}) RETURN id(a) as id", parameters("name", NAME))
					// Returns "only" a reactive streams Publisher
					.records()
				).map(r -> r.get("id").asLong())
				,
				RxSession::close);

			StepVerifier.create(personCreation)
				.expectNextCount(1)
				.verifyComplete();
		}

		@Test
		void txFunctions() {

			RxTransactionWork<Publisher<Long>> txFunction = tx ->
				Flux.from(
					tx.run("CREATE (a:Person {name: $name}) RETURN id(a) as id", parameters("name", NAME)).records()
				).map(r -> r.get("id").asLong());

			Flux<Long> personCreation = Flux.using(driver::rxSession,
				session -> session.writeTransaction(txFunction)
				, RxSession::close);

			StepVerifier.create(personCreation)
				.expectNextCount(1)
				.verifyComplete();
		}

		@Test
		void explicit() {

			Function<RxSession, Flux<Long>> actualWork = session ->
				Flux.usingWhen(session.beginTransaction(),
					// Yes, this looks pretty much like the `txFunction` in the example before
					tx -> Flux.from(
						tx.run("CREATE (a:Person {name: $name}) RETURN id(a) as id", parameters("name", NAME)).records()
					).map(r -> r.get("id").asLong()),
					RxTransaction::commit, // Success case
					(tx, e) -> tx.rollback(), // Error / exceptional case
					RxTransaction::commit); // Cancelation

			Flux<Long> personCreation = Flux.using(driver::rxSession, actualWork, RxSession::close);

			StepVerifier.create(personCreation)
				.expectNextCount(1)
				.verifyComplete();
		}
	}
}
