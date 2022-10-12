package com.kafka.java;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {
    private ConsumerDemoWithThreads(){

    }

    public static void main(String[] args) {
        new ConsumerDemoWithThreads().run();
    }
    private void run() {
        System.out.println("hello world");
        // create logger for this  class
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class);
        String bootstrapServer = "localhost:9092";
        String groupId = "java-two-group";
        // latch for dealing multiple threads
        CountDownLatch latch = new CountDownLatch(1);
        // create consumer runnable
        Runnable myConsumerRunnable = new ConsumerRunnable(latch,bootstrapServer,groupId);
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();
        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutDown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("application interrupted",e);
        }finally {
            logger.info("application is closing");
        }

    }
    public class ConsumerRunnable implements Runnable{
        private CountDownLatch latch;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);
        KafkaConsumer<String, String> kafkaConsumer;

        public ConsumerRunnable(CountDownLatch latch,String bootstrapServer,String groupId) {
            this.latch = latch;
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            kafkaConsumer = new KafkaConsumer<String, String>(properties);
            kafkaConsumer.subscribe(Arrays.asList("firsttopic"));
        }
        @Override
        public void run() {
            try {
                while (true){
                    ConsumerRecords<String,String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                    for(ConsumerRecord<String,String> r : records){
                        logger.info("Key: "+r.key());
                        logger.info("Partition: "+r.partition());
                        logger.info("Offset: "+r.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.error("received shutdown signal: ",e);
            }finally {
                kafkaConsumer.close();
                // tell our code that we're done with consumer
                latch.countDown();
            }
        }
        public void shutDown(){
            // the wakeup() method is a special method
            // it will throw wakeup exception
            kafkaConsumer.wakeup();
        }
    }
}
