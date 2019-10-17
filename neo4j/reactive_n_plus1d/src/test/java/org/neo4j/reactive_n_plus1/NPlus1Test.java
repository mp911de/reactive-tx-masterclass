package org.neo4j.reactive_n_plus1;

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.neo4j.driver.Driver;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.reactive.RxTransaction;
import org.neo4j.driver.types.Node;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Mostly
 * <strong>DON'T DO THIS AT HOME, THOSE ARE ANTIPATTERNS</strong>
 * in here.
 */
@SpringBootTest
class NPlus1Test {

	private final Driver driver;
	public static final String OUTER_QUERY = "MATCH (m:Movie) RETURN m ORDER BY m.title";
	public static final String INNER_QUERY = "MATCH (p:Person) - [:ACTED_IN] -> (m) WHERE id(m) = $movieId RETURN p.name AS name";

	@Autowired NPlus1Test(Driver driver) {
		this.driver = driver;
	}

	static class Movie {
		private final String title;

		private final List<Actor> actors;

		public Movie(String title, List<Actor> actors) {
			this.title = title;
			this.actors = actors;
		}

		@Override public String toString() {
			return "Movie{" +
				"title='" + title + '\'' +
				'}';
		}
	}

	static class Actor {
		private final String name;

		Actor(String name) {
			this.name = name;
		}
	}

	@Test
	void executeNPlus1QueriesSameSessionAutoCommit() {

		Flux<Movie> movies = Flux.using(driver::rxSession,
			session -> Flux.from(session.run(OUTER_QUERY).records())
				.flatMap(record -> {
					Node node = record.get("m").asNode();
					return Flux.from(session.run(INNER_QUERY, Collections.singletonMap("movieId", node.id())).records())
						.map(r -> new Actor(r.get("name").asString()))
						.collectList()
						.map(actors -> new Movie(node.get("title").asString(), actors));
				}),
			RxSession::close);

		StepVerifier.create(movies)
			.thenConsumeWhile(m -> true, System.out::println)
			.verifyComplete();
	}

	@Test
	void executeNPlus1QueriesSameSessionAutoCommitV2() {

		Flux<Movie> movies = Flux.using(driver::rxSession,
			session -> Flux.from(session.run(OUTER_QUERY).records()),
			RxSession::close)
			.limitRate(1)
			.flatMap(record -> {
				Node node = record.get("m").asNode();
				return Flux.using(driver::rxSession,
					session -> Flux
						.from(session.run(INNER_QUERY, Collections.singletonMap("movieId", node.id())).records())
						.map(r -> new Actor(r.get("name").asString()))
						.collectList()
						.map(actors -> new Movie(node.get("title").asString(), actors)),
					RxSession::close);
			});

		StepVerifier.create(movies)
			.thenConsumeWhile(m -> true, System.out::println)
			.verifyComplete();
	}

	@Test
	void executeNPlus1QueriesDifferentSessionAutoCommit() {

		Flux<Movie> movies = Flux.using(driver::rxSession,
			session -> Flux.from(session.run(OUTER_QUERY).records())
				// .limitRate(1)
				.flatMap(record -> {
					Node node = record.get("m").asNode();

					return Flux.using(driver::rxSession, innerSession -> Flux
						.from(innerSession.run(INNER_QUERY, Collections.singletonMap("movieId", node.id())).records())
						//.limitRate(1)
						.map(r -> new Actor(r.get("name").asString()))
						.collectList()
						.map(actors -> new Movie(node.get("title").asString(), actors)), RxSession::close);
				}),
			RxSession::close);

		StepVerifier.create(movies)
			.thenConsumeWhile(m -> true, System.out::println)
			.verifyComplete();
	}

	@Test
	void executeNPlus1QueriesSameSessionTXFunction() {

		Flux<Movie> movies = Flux.using(driver::rxSession,
			session -> Flux.from(session.readTransaction(tx -> tx.run(OUTER_QUERY).records()))
				.limitRate(1)
				.flatMap(record -> {
					Node node = record.get("m").asNode();

					return Flux.from(session.readTransaction(
						tx -> tx.run(INNER_QUERY, Collections.singletonMap("movieId", node.id())).records()))
						.limitRate(1)
						.map(r -> new Actor(r.get("name").asString()))
						.collectList()
						.map(actors -> new Movie(node.get("title").asString(), actors));
				}),
			RxSession::close);

		StepVerifier.create(movies)
			.thenConsumeWhile(m -> true, System.out::println)
			.verifyComplete();
	}

	@Test
	void executeNPlus1QueriesDifferentSessionTXFunction() {

		Flux<Movie> movies = Flux.using(driver::rxSession,
			session -> Flux.from(session.readTransaction(tx -> tx.run(OUTER_QUERY).records()))
				.limitRate(1)
				.flatMap(record -> {
					Node node = record.get("m").asNode();

					return Flux.using(driver::rxSession, innerSession -> Flux.from(innerSession.readTransaction(
						tx -> tx.run(INNER_QUERY, Collections.singletonMap("movieId", node.id())).records()))
						.limitRate(1)
						.map(r -> new Actor(r.get("name").asString()))
						.collectList()
						.map(actors -> new Movie(node.get("title").asString(), actors)), RxSession::close);
				}),
			RxSession::close);

		StepVerifier.create(movies)
			.thenConsumeWhile(m -> true, System.out::println)
			.verifyComplete();
	}

	/**
	 * Optimal version (as optimal as those queries get)
	 */
	@Test
	void executeNPlus1QueriesSameTX() {

		Flux<Movie> movies = Flux.using(driver::rxSession,
			session -> Flux.usingWhen(session.beginTransaction(),
				tx -> Flux.from(tx.run(OUTER_QUERY).records())
					.limitRate(1)
					.flatMap(record -> {
						Node node = record.get("m").asNode();
						return Flux
							.from(tx.run(INNER_QUERY, Collections.singletonMap("movieId", node.id())).records())
							.limitRate(1)
							.map(r -> new Actor(r.get("name").asString()))
							.collectList()
							.map(actors -> new Movie(node.get("title").asString(), actors));
					}),
				RxTransaction::commit,
				(tx, e) -> tx.rollback(),
				RxTransaction::rollback),
			RxSession::close);

		StepVerifier.create(movies)
			.thenConsumeWhile(m -> true, System.out::println)
			.verifyComplete();
	}
}
