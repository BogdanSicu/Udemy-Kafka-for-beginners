package org.example.io.conduktor.demos.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());
    public static void main(String[] args) {
        log.info("-----------------I am a Kafka Producer----------------------");

//region create Producer Properties
        Properties properties = new Properties();

        // connect to Localhost
        properties.setProperty("bootstrap.servers", "localhost:9092");

        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

//endregion

//region create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int j=0;j<2;j++) {
            for (int i = 0; i < 30; i++) {

                String topic = "demo_java";
                String key = "id_" + i;
                String value = "Hello World " + i;

                //create Producer Record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                //region send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        // executes every time a record is successfully sent or an exception is thrown
                        if (exception == null) {
                            //the record was successfully sent
                            log.info("Key: " + key + " | Partition: " + metadata.partition());
                        } else {
                            log.error("Error while producing", exception);
                        }
                    }
                });
                //endregion
            }
        }
//endregion


//region flush and close the producer
        // tell the producer to send all data and block until done -- synchronous operation
        producer.flush();

        producer.close(); // this also calls producer.flush() before closing
//endregion
    }
}
