package org.neo4j.sdn_rx;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.neo4j.springframework.data.repository.ReactiveNeo4jRepository;
import org.neo4j.springframework.data.repository.query.Query;

public interface PersonRepository extends ReactiveNeo4jRepository<PersonEntity, String> {

	Mono<PersonEntity> findByName(String name);

	@Query("MATCH (am:Movie)<-[ai:ACTED_IN]-(p:Person)-[d:DIRECTED]->(dm:Movie) return p, collect(ai), collect(d), collect(am), collect(dm)")
	Flux<PersonEntity> getPersonsWhoActAndDirect();
}
