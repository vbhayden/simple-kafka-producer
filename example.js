const kafkaProducer = require("./index");

// Set everything up
kafkaProducer.configure({
    brokers: "127.0.0.1:9092",
    saslUser: "insecure",
    saslPass: "credentials"
})

// Assign some optional callback to use for after a message is sent
kafkaProducer.setCallback(function(topic, offset, message) {
    console.log(`[Kafka Producer Example]: ${topic}@${offset}: ${message}`)
});

// Need to call this to trigger the client's startup process.
kafkaProducer.initProducer();

// You should call this after the producer's been initialized, but there's
// a cache that collects offline messages and then gets dumped when the producer 
// starts up.
kafkaProducer.produceMessage("test-1", "a message!");