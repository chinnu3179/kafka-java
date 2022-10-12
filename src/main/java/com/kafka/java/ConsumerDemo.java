package com.kafka.java;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {
        System.out.println("hello world");
        // create logger for this  class
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
        // create consumer properties
        Properties properties = new Properties();
        String bootstrapServer = "localhost:9092";
        String groupId = "java-group";
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        // create consumer
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        // subscribe to our topics
        kafkaConsumer.subscribe(Arrays.asList("firsttopic"));

        // poll data

        while (true){
            ConsumerRecords<String,String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> r : records){
                logger.info("Key: "+r.key());
                logger.info("Partition: "+r.partition());
                logger.info("Offset: "+r.offset());
            }
        }

    }
}
