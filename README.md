## Simplified Kafka Producer
This is a Node module to streamline Kafka integration for Node services.  It's intended to provide a common and heavily-abstracted Kafka client to use for producing messages into the Kafka cluster.

This is essentially a wrapper for the **[kafka-node](https://www.npmjs.com/package/kafka-node)** library.

### Installation
You can install this through NPM:
```
npm install simple-kafka-producer --save
```

### Setup
Setup for a producer is pretty minimal.  This adapter supports plaintext SASL as an authorization mechanism, so you'll need to provide those credentials if that's how your cluster is configured.

```
const kafkaProducer = require("simple-kafka-producer");

// Set everything up
kafkaProducer.configure({
    brokers: "localhost:9092",
    saslUser: "insecure",
    saslPass: "credentials"
})
```

Once you've assigned your Kafka information, you'll need to initialize the client before you can send anything.

```
kafkaProducer.initProducer();
```

After that, you should be able to send messages to a given Kafka topic with:

```
kafkaProducer.produceMessage("test-1", "a message!");
```

You can set an optional callback with the producer that will be called after a message is produced, containing its Kafka offset:
```
kafkaProducer.setCallback(function(topic, offset, message) {
    console.log(`[Kafka Producer Example]: ${topic}@${offset}: ${message}`)
});
```

That's all there is to it.

### Failure Tolerance
This adapter is set up to cache any messages that failed to send and try to resend them after a short delay.  You shouldn't need to handle any of this yourself.  Do note:
- If the connection to your Kafka cluster is lost, then the adapter will begin caching all messages and will attemtp to send them / flush its cache in reconnection.  
- If for some reason the service is killed during this process, **those cached messages will be lost**.
