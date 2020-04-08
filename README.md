# quarkus-reactive-jms-tx

Quarkus reactive JMS Session `SESSION_TRANSACTED` receiver.

```java
@Incoming("input")
@Acknowledgment(Acknowledgment.Strategy.MANUAL)
public CompletionStage<Void> message(InputJmsMessage<Data> input) {
    try {    
      ...
      return input.ack();
    } catch (Exception ex) {
        return input.rollback();
    }
}
```
