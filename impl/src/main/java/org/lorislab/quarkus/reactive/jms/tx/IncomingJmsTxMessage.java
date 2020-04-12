package org.lorislab.quarkus.reactive.jms.tx;

import io.smallrye.reactive.messaging.jms.IncomingJmsMessageMetadata;
import io.smallrye.reactive.messaging.jms.JmsProperties;
import io.smallrye.reactive.messaging.jms.JmsPropertiesBuilder;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import javax.jms.*;
import javax.json.bind.Jsonb;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.stream.Stream;

public class IncomingJmsTxMessage<T> implements org.eclipse.microprofile.reactive.messaging.Message<T> {

    public static String MESSAGE_PROPERTY_CLASSNAME = "_classname";

    private final Message delegate;
    private final Executor executor;
    private final Class<T> clazz;
    private final Jsonb json;
    private final IncomingJmsMessageMetadata jmsMetadata;
    private final Metadata metadata;
    private final JMSContext context;

    IncomingJmsTxMessage(JMSContext context, Message message, Executor executor, Jsonb json) {
        this.delegate = message;
        this.json = json;
        this.executor = executor;
        this.context = context;

        String cn = null;
        try {
            cn = message.getStringProperty(MESSAGE_PROPERTY_CLASSNAME);
            if (cn == null) {
                cn = message.getJMSType();
            }
        } catch (JMSException e) {
            // ignore it
        }
        try {
            this.clazz = cn != null ? load(cn) : null;
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Unable to load the class " + e);
        }

        this.jmsMetadata = new IncomingJmsMessageMetadata(message);
        this.metadata = Metadata.of(this.jmsMetadata);
    }

    @SuppressWarnings("unchecked")
    private Class<T> load(String cn) throws ClassNotFoundException {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        if (loader != null) {
            try {
                return (Class<T>) loader.loadClass(cn);
            } catch (ClassNotFoundException e) {
                // Will try with the current class classloader
            }
        }
        return (Class<T>) IncomingJmsTxMessage.class.getClassLoader().loadClass(cn);
    }

    public JMSContext getContext() {
        return context;
    }

    public IncomingJmsMessageMetadata getJmsMetadata() {
        return jmsMetadata;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T getPayload() {
        try {
            if (clazz != null) {
                return convert(delegate.getBody(String.class));
            } else {
                return (T) delegate.getBody(Object.class);
            }
        } catch (JMSException e) {
            throw new java.lang.IllegalStateException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private T convert(String value) {
        if (clazz.equals(Integer.class)) {
            return (T) Integer.valueOf(value);
        }
        if (clazz.equals(Long.class)) {
            return (T) Long.valueOf(value);
        }
        if (clazz.equals(Double.class)) {
            return (T) Double.valueOf(value);
        }
        if (clazz.equals(Float.class)) {
            return (T) Float.valueOf(value);
        }
        if (clazz.equals(Boolean.class)) {
            return (T) Boolean.valueOf(value);
        }
        if (clazz.equals(Short.class)) {
            return (T) Short.valueOf(value);
        }
        if (clazz.equals(Byte.class)) {
            return (T) Byte.valueOf(value);
        }
        if (clazz.equals(String.class)) {
            return (T) value;
        }

        return json.fromJson(value, clazz);
    }

    @Override
    public CompletionStage<Void> ack() {
        return CompletableFuture.runAsync(context::commit, executor);
    }

    public CompletionStage<Void> rollback() {
        return CompletableFuture.runAsync(context::rollback, executor);
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public <C> C unwrap(Class<C> unwrapType) {
        if (Message.class.equals(unwrapType)) {
            return (C) delegate;
        }
        if (IncomingJmsMessageMetadata.class.equals(unwrapType)) {
            return (C) jmsMetadata;
        }
        throw new IllegalArgumentException("Unable to unwrap message to " + unwrapType);
    }

    public void send(Stream<org.eclipse.microprofile.reactive.messaging.Message<?>> messages) {
        JMSProducer producer = context.createProducer();
        messages.forEach(m -> send(context, json, producer, m));
    }

    private static void send(JMSContext context, Jsonb json, JMSProducer producer, org.eclipse.microprofile.reactive.messaging.Message<?> message) {
        OutgoingJmsTxMessageMetadata outgoingMetadata = message.getMetadata(OutgoingJmsTxMessageMetadata.class).orElse(null);
        if (outgoingMetadata == null) {
            throw new java.lang.IllegalStateException("Missing metadata in outgoing message");
        }
        String dest = outgoingMetadata.getDestination();
        if (dest == null || dest.isBlank()) {
            throw new java.lang.IllegalStateException("Missing destination name in outgoing message");
        }

        try {
            Message outgoing = createJmsMessage(context, json, message.getPayload());

            String correlationId = outgoingMetadata.getCorrelationId();
            if (correlationId != null) {
                outgoing.setJMSCorrelationID(correlationId);
            }

            int deliveryMode = outgoingMetadata.getDeliveryMode();
            if (deliveryMode != -1) {
                outgoing.setJMSDeliveryMode(deliveryMode);
            }

            String type = outgoingMetadata.getType();
            if (type != null) {
                outgoing.setJMSType(type);
            }

            String replyTo = outgoingMetadata.getReplyTo();
            if (replyTo != null && !replyTo.isEmpty()) {
                outgoing.setJMSReplyTo(getDestination(context, replyTo, type));
            }

            JmsProperties properties = outgoingMetadata.getProperties();
            if (properties != null) {
                if (!(properties instanceof JmsPropertiesBuilder.OutgoingJmsProperties)) {
                    throw new javax.jms.IllegalStateException("Unable to map JMS properties to the outgoing message, "
                            + "OutgoingJmsProperties expected, found " + properties.getClass().getName());
                }
                JmsPropertiesBuilder.OutgoingJmsProperties op = ((JmsPropertiesBuilder.OutgoingJmsProperties) properties);
                op.getProperties().forEach(p -> p.apply(outgoing));
            }

            Destination destination = getDestination(context, dest, type);
            producer.send(destination, outgoing);
        } catch (Exception ex) {
            throw new java.lang.IllegalStateException("Error send the message!", ex);
        }
    }

    private static Destination getDestination(JMSContext context, String name, String type) {
        switch (type.toLowerCase()) {
            case OutgoingJmsTxMessageMetadata.OutputJmsTxMessageMetadataBuilder.QUEUE:
                return context.createQueue(name);
            case OutgoingJmsTxMessageMetadata.OutputJmsTxMessageMetadataBuilder.TOPIC:
                return context.createTopic(name);
            default:
                throw new IllegalArgumentException("Unknown destination type: " + type);
        }
    }

    public static Message createJmsMessage(JMSContext context, Jsonb json, Object payload) throws JMSException {
        javax.jms.Message outgoing;
        if (payload instanceof String || payload.getClass().isPrimitive() || isPrimitiveBoxed(payload.getClass())) {
            outgoing = context.createTextMessage(payload.toString());
            outgoing.setStringProperty(MESSAGE_PROPERTY_CLASSNAME, payload.getClass().getName());
            outgoing.setJMSType(payload.getClass().getName());
        } else if (payload.getClass().isArray() && payload.getClass().getComponentType().equals(Byte.TYPE)) {
            BytesMessage o = context.createBytesMessage();
            o.writeBytes((byte[]) payload);
            outgoing = o;
        } else {
            try {
                outgoing = context.createTextMessage(json.toJson(payload));
            } catch (Exception ex) {
                throw new java.lang.IllegalStateException(ex);
            }
            outgoing.setJMSType(payload.getClass().getName());
            outgoing.setStringProperty(MESSAGE_PROPERTY_CLASSNAME, payload.getClass().getName());
        }
        return outgoing;
    }

    private static boolean isPrimitiveBoxed(Class<?> c) {
        return c.equals(Boolean.class)
                || c.equals(Integer.class)
                || c.equals(Byte.class)
                || c.equals(Double.class)
                || c.equals(Float.class)
                || c.equals(Short.class)
                || c.equals(Character.class)
                || c.equals(Long.class);
    }
}
