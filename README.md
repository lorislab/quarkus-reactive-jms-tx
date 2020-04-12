# quarkus-reactive-jms-tx

Quarkus reactive JMS Session `SESSION_TRANSACTED` receiver.

[![License](https://img.shields.io/github/license/lorislab/quarkus-reactive-jms-tx?style=for-the-badge&logo=apache)](https://www.apache.org/licenses/LICENSE-2.0)
[![GitHub Workflow Status (branch)](https://img.shields.io/github/workflow/status/lorislab/quarkus-reactive-jms-tx/master/master?logo=github&style=for-the-badge)](https://github.com/lorislab/quarkus-reactive-jms-tx/actions?query=workflow%3Amaster)
[![GitHub tag (latest SemVer)](https://img.shields.io/github/v/tag/lorislab/quarkus-reactive-jms-tx?logo=github&style=for-the-badge)](https://github.com/lorislab/quarkus-reactive-jms-tx/releases/latest)
[![Maven Central](https://img.shields.io/maven-central/v/org.lorislab.quarkus/quarkus-reactive-jms-tx?logo=java&style=for-the-badge)](https://maven-badges.herokuapp.com/maven-central/org.lorislab.quarkus/quarkus-reactive-jms-tx)

### Example

```java
@Incoming("input")
@Acknowledgment(Acknowledgment.Strategy.MANUAL)
public CompletionStage<Void> message(IncomingJmsTxMessage<Data> input) {
    try {    
      ...
      // optional send message
      input.send(Message.of("Output"));
        
      // session commit
      return input.ack();
    } catch (Exception ex) {
        // session rollback
        return input.rollback();
    }
}
```
