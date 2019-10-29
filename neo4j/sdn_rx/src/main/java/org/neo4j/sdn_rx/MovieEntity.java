package org.neo4j.sdn_rx;

import static org.neo4j.springframework.data.core.schema.Relationship.Direction.*;

import java.util.Set;

import org.neo4j.springframework.data.core.schema.Id;
import org.neo4j.springframework.data.core.schema.Node;
import org.neo4j.springframework.data.core.schema.Property;
import org.neo4j.springframework.data.core.schema.Relationship;

@Node("Movie")
public class MovieEntity {

	@Id
	private final String title;

	@Property("tagline")
	private final String description;

	@Relationship(type = "ACTED_IN", direction = INCOMING)
	private Set<PersonEntity> actors;

	@Relationship(type = "DIRECTED", direction = INCOMING)
	private Set<PersonEntity> directors;

	public MovieEntity(String title, String description) {
		this.title = title;
		this.description = description;
	}

	public String getTitle() {
		return title;
	}

	public String getDescription() {
		return description;
	}

	public Set<PersonEntity> getActors() {
		return actors;
	}

	public Set<PersonEntity> getDirectors() {
		return directors;
	}
}
