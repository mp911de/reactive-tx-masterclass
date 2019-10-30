package rxtx.extension;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Logging;
import org.neo4j.harness.internal.InProcessNeo4j;
import org.neo4j.harness.internal.Neo4jBuilder;
import org.neo4j.harness.internal.TestNeo4jBuilders;
import org.neo4j.harness.junit.extension.Neo4j;

public class Neo4jDriverExtension implements BeforeAllCallback, AfterAllCallback, ParameterResolver {

	private static final ExtensionContext.Namespace NEO4J = ExtensionContext.Namespace.create("NEO4J");
	private final Neo4jBuilder builder;

	public Neo4jDriverExtension() {
		this.builder = TestNeo4jBuilders.newInProcessBuilder();
	}

	public void beforeAll(ExtensionContext context) {

		InProcessNeo4j neo4j = this.builder.build();
		context.getStore(NEO4J).put(Neo4j.class, neo4j);
		Config config = Config.builder().withLogging(Logging.slf4j()).build();
		context.getStore(NEO4J).put(Driver.class, GraphDatabase.driver(neo4j.boltURI(), config));
	}

	public void afterAll(ExtensionContext context) {

		Store store = context.getStore(NEO4J);

		Driver driver = store.remove(Driver.class, Driver.class);
		driver.close();

		InProcessNeo4j neo4j = store.remove(Neo4j.class, InProcessNeo4j.class);
		neo4j.close();
	}

	public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
		throws ParameterResolutionException {
		Class<?> paramType = parameterContext.getParameter().getType();
		return paramType.equals(Driver.class);
	}

	public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
		throws ParameterResolutionException {
		Class<?> paramType = parameterContext.getParameter().getType();
		return extensionContext.getStore(NEO4J).get(paramType, paramType);
	}
}
