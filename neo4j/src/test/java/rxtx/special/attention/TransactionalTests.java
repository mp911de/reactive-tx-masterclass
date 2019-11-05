package rxtx.special.attention;

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnJre;
import org.junit.jupiter.api.condition.JRE;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Logging;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.harness.internal.TestNeo4jBuilders;
import org.neo4j.harness.junit.extension.Neo4j;
import org.neo4j.springframework.data.config.AbstractReactiveNeo4jConfig;
import org.neo4j.springframework.data.core.ReactiveNeo4jClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.reactive.TransactionalOperator;

@ExtendWith(SpringExtension.class)
@DisabledOnJre({ JRE.JAVA_8, JRE.JAVA_9, JRE.JAVA_10 }) // Neo4j 4.0 embedded is JDK 11+
public class TransactionalTests {

	@BeforeEach
	void clearDatabase(@Autowired Driver driver) {

		Flux.using(driver::rxSession,
			session -> session.run("MATCH (n) DETACH DELETE n").summary(),
			RxSession::close
		).then().as(StepVerifier::create).verifyComplete();
	}

	@Test
	void nPlusOne(@Autowired ReactiveNeo4jClient client, @Autowired TransactionalOperator rxtx) {

		long expectNumberOfNodes = 1000L;
		client.query("UNWIND range (1,$upper) AS i RETURN i")
			.bind(expectNumberOfNodes).to("upper")
			.fetchAs(Long.class)
			.all()
			.limitRate(1)
			.flatMap(it -> client.query("CREATE (n:Node {pos: $i}) RETURN n").bind(it).to("i").fetch().all())
			//	.as(rxtx::transactional)
			.as(StepVerifier::create).expectNextCount(expectNumberOfNodes).verifyComplete();
	}

	@Configuration
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

			Config config = Config.builder().withLogging(Logging.slf4j())
				//		.withMaxConnectionPoolSize(1)
				.build();
			return GraphDatabase.driver(neo4j().boltURI(), config);
		}
	}
}
