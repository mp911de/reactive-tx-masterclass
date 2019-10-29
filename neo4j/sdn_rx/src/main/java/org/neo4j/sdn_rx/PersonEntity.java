package org.neo4j.sdn_rx;

import org.neo4j.springframework.data.core.schema.Id;
import org.neo4j.springframework.data.core.schema.Node;

@Node("Person")
public class PersonEntity {

	@Id
	private final String name;

	private Long born;

	public PersonEntity(String name, Long born) {
		this.born = born;
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public Long getBorn() {
		return born;
	}

	public void setBorn(Long born) {
		this.born = born;
	}
}
