package com.kafka.java;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithAssignSeek {
    public static void main(String[] args) {
        System.out.println("hello world");
        // create logger for this  class
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithAssignSeek.class);
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

        // assign and seek are mostly used to replay data or fetch a specific message

        // assign
        TopicPartition partitionToReadFrom = new TopicPartition("firsttopic",0);
        kafkaConsumer.assign(Arrays.asList(partitionToReadFrom));

        //seek
        long offsetToReadFrom = 50L;
        kafkaConsumer.seek(partitionToReadFrom,offsetToReadFrom);

        // poll data
        int messagesToRead = 5;
        boolean keepReading = true;
        int messagesRead = 0;
        while (keepReading){
            ConsumerRecords<String,String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> r : records){
                messagesRead++;
                logger.info("Key: "+r.key());
                logger.info("Partition: "+r.partition());
                logger.info("Offset: "+r.offset());
               if( messagesRead>=5 ){
                   keepReading = false;
                   break;
               }
            }

        }

    }
}
