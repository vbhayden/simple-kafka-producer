const kafka = require("kafka-node");

/**
 * @typedef {Object} SimpleKafkaProducerConfig
 * @property {string} brokers A comma-delimited string of Kafka broker endpoints.
 * @property {boolean} useSasl Whether or not to use SASL as the auth.
 * @property {string} saslUser SASL user for the cluster.
 * @property {string} saslPass SASL password for the cluster.
 */
/** Configuration settings for our client. 
 * @type {SimpleKafkaProducerConfig}
*/
var config = undefined;

/**
 * Callback for adding two numbers.
 *
 * @callback onProduce
 * @param {string} topic - Message produced to the given Kafka topic.
 * @param {string} offset - Message produced to the given Kafka topic.
 * @param {string} message - Message produced to the given Kafka topic.
 */
var callback = undefined;

/**
 * Kafka Producer instance.
 * 
 * If our Producer goes down for some reason, we will want that back up to continue monitoring, 
 * but the work to do this is a bit clunky.
 * @type {kafka.Producer}
 */
var producer = undefined;

/**
 * Kafka Client to interface with our cluster.
 * @type {kafka.KafkaClient}
 */
var client = undefined;

/**
 * Backlog of messages we intended to send, but could not becaue of a downage.
 * @type {{topic: string, message: string}[]}
 */
var backlog = []

// Reset our consumer if anything should go wrong, this will clear out the existing consumer,
// including its callbacks, and recreate one in its place.
//
function resetClient() {

    console.log("[Kafka] Reconnecting Client ...");
    producer = undefined;

    let resetAttempt = () => {
        try {
            if (client != undefined) {
                client.close(initProducer);
            } else {
                initProducer();
            }
        } 
        catch (err) {
            console.log("[Kafka] Could not reset, trying again soon ...")
            setTimeout(resetAttempt, 1000);
        }
    }

    resetAttempt();
}

// Broadcast that something happened.  This will give us a common failure message for either the try/catch
// block or the Consumer itself.
//
function onFailure(error) {

    console.log("[Kafka] Producer attempting reset ...");
    resetClient();
}

// Initialize the consumer with its callbacks and whatnot.
function initProducer() {

    client = new kafka.KafkaClient({
        kafkaHost: config.brokers,
        sasl: (config.useSasl !== true ? undefined : {
            mechanism: "plain",
            username: config.saslUser,
            password: config.saslPass
        }),
    });

    console.log(`
    \r[Kafka]: Targeting Kafka cluster with SASL credentials:
    \r    SASL USER: ${config.saslUser},
    \r    SASL PASS: ${config.saslPass},
    \r    CLUSTER  : ${config.brokers}
    `);

    producer = new kafka.Producer(client);
    producer.flying = 0

    producer.on("error", onFailure);
    producer.on("ready", function() {

        console.log("[Kafka] Producer initialized, will target: ", config.brokers); 
        checkBacklog();
    });
}

/**
 * Produce a message to a given Kafka topic.
 * @param {string} topic Topic to target.
 * @param {(object|string)} message Message to produce.
 * @param {number} partition Partition to use.
 */
function produceMessage(topic, message, partition = 0) {
    
    let payload = [{
        topic: topic,
        partition: partition,
        messages: [typeof message == "object" ? JSON.stringify(message) : message]
    }]

    // Check if we don't have a producer atm or if we started the failure process
    if (producer == undefined || producer.failing) {
        addToBacklog(topic, message)
    } 
    
    // Otherwise, send it normally
    else {

        // Update our flight counter prior to sending this message
        producer.flying++;
        producer.send(payload, (err, data) => {

            // Decrement to clean up our flight counter then check if we had any issues
            producer.flying--;

            if (err) {
                
                // If we got an error here, trigger the failure
                if (!producer.failing) {
                    producer.failing = true
                    console.log("[Kafka] Producer failure detected, waiting for reset ...");
                }

                // Add this message to our backlog as it didn't get through
                addToBacklog(topic, message, partition)
                
                // Only trigger a reconnect if we didn't have anything in-flight
                if (producer.flying == 0)
                    resetClient();
    
            } else {
                
                // If we're here, then everything's fine.  Get the offset and check for a callback.
                let offset = data[topic]['0'];
                
                if (callback != undefined)
                    callback(topic, offset, message)
                else
                    console.log(`[Kafka] Produced to topic ${topic} at offset ${offset}`)
            }
        })
    }
}

// Add to our backlog
function addToBacklog(topic, message, partition) {

    backlog.push({topic: topic, message: message, partition: partition})
    console.log(`[Kafka] Backlogging message for ${topic} (Total: ${backlog.length}): Message: ${message}`)
}

// Reset our backlog
function checkBacklog() {

    if (backlog.length == 0) {
        console.log(`[Kafka] No backlog to clear.`)

    } else {
        let cache = JSON.parse(JSON.stringify(backlog))
        backlog = []
    
        console.log(`[Kafka] Producer dumping (${cache.length}) backlogged messages ...`);
        dumpCache(cache, 250);
    }
}

// Remove our backlog over time.  This isn't a particularly robust solution, but it should
// be fine as a stopgap.  If our Kafka cluster keeps going down or is consistently unreachable,
// then that's its own problem.
//
function dumpCache(cache, batchSize) {

    // Keep track of how many times we've been through this loop
    let iteration = 0;
    let handle = setInterval(function(){

        // Set up our bounds
        let lower = batchSize * iteration;
        let upper = batchSize * (iteration + 1)
        
        // If our upper bound exceeds our cache length, then this is our last iteration
        if (upper >= cache.length) {
            upper = cache.length
            clearInterval(handle);
        }

        // Just go through and send each message
        for (let k=lower; k<upper; k++) {
            let msg = cache[k]
            produceMessage(msg.topic, msg.message, msg.partition);
        }
    }, 500);
    console.log(`[Kafka] ... backlog emptied.`)
}

module.exports = {
    initProducer,
    produceMessage,

    /**
     * Set the callback to fire when a statement is produced to Kafka.
     * @param {onProduce} cb 
     */
    setCallback: function(cb) {
        callback = cb;
    },

    /**
     * Configure the producer.  This is required before you can actually do anything.
     * @param {SimpleKafkaProducerConfig} configObj 
     */
    configure: function(configObj) {
        config = configObj;
    }
}