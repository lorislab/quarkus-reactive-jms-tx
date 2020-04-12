package org.lorislab.quarkus.reactive.jms.tx;

import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.literal.NamedLiteral;
import javax.inject.Inject;
import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;

@ApplicationScoped
@Connector(JmsTxConnector.CONNECTOR_NAME)

@ConnectorAttribute(name = "connection-factory-name", description = "The name of the JMS connection factory  (`javax.jms.ConnectionFactory`) to be used. If not set, it uses any exposed JMS connection factory", direction = Direction.INCOMING, type = "String")
@ConnectorAttribute(name = "username", description = "The username to connect to to the JMS server", direction = Direction.INCOMING, type = "String")
@ConnectorAttribute(name = "password", description = "The password to connect to to the JMS server", direction = Direction.INCOMING, type = "String")
@ConnectorAttribute(name = "client-id", description = "The client id", direction = Direction.INCOMING, type = "String")
@ConnectorAttribute(name = "destination", description = "The name of the JMS destination. If not set the name of the channel is used", direction = Direction.INCOMING, type = "String")
@ConnectorAttribute(name = "selector", description = "The JMS selector", direction = Direction.INCOMING, type = "String")
@ConnectorAttribute(name = "no-local", description = "Enable or disable local delivery", direction = Direction.INCOMING, type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "broadcast", description = "Whether or not the JMS message should be dispatched to multiple consumers", direction = Direction.INCOMING, type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "durable", description = "Set to `true` to use a durable subscription", direction = Direction.INCOMING, type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "destination-type", description = "The type of destination. It can be either `queue` or `topic`", direction = Direction.INCOMING, type = "string", defaultValue = "queue")

public class JmsTxConnector implements IncomingConnectorFactory {

    /**
     * The name of the connector: {@code lorislab-jms-tx}
     */
    public static final String CONNECTOR_NAME = "lorislab-jms-tx";

    /**
     * The default max-pool-size: {@code 10}
     */
    @SuppressWarnings("WeakerAccess")
    static final String DEFAULT_MAX_POOL_SIZE = "10";

    /**
     * The default thread ideal TTL: {@code 60} seconds
     */
    @SuppressWarnings("WeakerAccess")
    static final String DEFAULT_THREAD_TTL = "60";

    @Inject
    Instance<ConnectionFactory> factories;

    @Inject
    Instance<Jsonb> jsonb;

    @Inject
    @ConfigProperty(name = "lorislab.jms.threads.max-pool-size", defaultValue = DEFAULT_MAX_POOL_SIZE)
    int maxPoolSize;

    @Inject
    @ConfigProperty(name = "lorislab.jms.threads.ttl", defaultValue = DEFAULT_THREAD_TTL)
    int ttl;

    private ExecutorService executor;
    private Jsonb json;
    private final List<JmsTxSource> sources = new CopyOnWriteArrayList<>();
    private final List<JMSContext> contexts = new CopyOnWriteArrayList<>();

    @PostConstruct
    public void init() {
        this.executor = new ThreadPoolExecutor(0, maxPoolSize, ttl, TimeUnit.SECONDS, new SynchronousQueue<>());
        if (jsonb.isUnsatisfied()) {
            this.json = JsonbBuilder.create();
        } else {
            this.json = jsonb.get();
        }

    }

    @PreDestroy
    public void cleanup() {
        sources.forEach(JmsTxSource::close);
        contexts.forEach(JMSContext::close);
        this.executor.shutdown();
    }

    @Override
    public PublisherBuilder<? extends Message<?>> getPublisherBuilder(Config config) {
        JmsTxConnectorIncomingConfiguration ic = new JmsTxConnectorIncomingConfiguration(config);
        JMSContext context = createJmsContext(ic);
        contexts.add(context);

        // add the context to map
        JmsTxSource source = new JmsTxSource(context, ic, json, executor);
        sources.add(source);
        return source.getSource();
    }

    private JMSContext createJmsContext(JmsTxConnectorIncomingConfiguration config) {
        String factoryName = config.getConnectionFactoryName().orElse(null);
        ConnectionFactory factory = pickTheFactory(factoryName);
        JMSContext context = createContext(factory,
                config.getUsername().orElse(null),
                config.getPassword().orElse(null));
        config.getClientId().ifPresent(context::setClientID);
        return context;
    }

    private ConnectionFactory pickTheFactory(String factoryName) {
        if (factories.isUnsatisfied()) {
            if (factoryName == null) {
                throw new IllegalStateException("Cannot find a javax.jms.ConnectionFactory bean");
            } else {
                throw new IllegalStateException("Cannot find a javax.jms.ConnectionFactory bean named " + factoryName);
            }
        }

        Iterator<ConnectionFactory> iterator;
        if (factoryName == null) {
            iterator = factories.iterator();
        } else {
            iterator = factories.select(NamedLiteral.of(factoryName)).iterator();
        }

        if (!iterator.hasNext()) {
            if (factoryName == null) {
                throw new IllegalStateException("Cannot find a javax.jms.ConnectionFactory bean");
            } else {
                throw new IllegalStateException("Cannot find a javax.jms.ConnectionFactory bean named " + factoryName);
            }
        }

        return iterator.next();
    }

    private JMSContext createContext(ConnectionFactory factory, String username, String password) {
        if (username != null) {
            return factory.createContext(username, password, JMSContext.SESSION_TRANSACTED);
        } else {
            return factory.createContext(JMSContext.SESSION_TRANSACTED);
        }
    }
}
