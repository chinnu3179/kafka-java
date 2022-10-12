package com.kafka.java;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProduerDemo {
    public static void main(String[] args) {
        System.out.println("hello world");
        // create producer properties
        // send data
        Properties properties = new Properties();
        String bootstrapServer = "localhost:9092";
//        properties.setProperty("bootstrap.server",bootstrapServer);
//        properties.setProperty("key.serializer", StringSerializer.class.getName());
//        properties.setProperty("value.serializer",StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // create producer
        KafkaProducer<String,String >  producer = new KafkaProducer<String, String>(properties);

        // crete a producer record
        ProducerRecord<String,String> record = new ProducerRecord<>("firsttopic","kafka from java");

        //send kafka data
        producer.send(record);
        producer.flush();
        producer.close();

    }
}
