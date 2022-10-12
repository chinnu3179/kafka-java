package com.kafka.java;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProduerDemoWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.out.println("hello world");
        //create logger for this class
        Logger logger = LoggerFactory.getLogger(ProduerDemoWithKeys.class);
        // create producer properties
        // send data
        Properties properties = new Properties();
        String bootstrapServer = "localhost:9092";
//        properties.setProperty("bootstrap.server",bootstrapServer);
//        properties.setProperty("key.serializer", StringSerializer.class.getName());
//        properties.setProperty("value.serializer",StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,"5");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // create producer
        KafkaProducer<String,String >  producer = new KafkaProducer<String, String>(properties);

        // crete a producer record
        for (int i=0;i<20;i++) {
            String key = "id_"+Integer.toString(i);
            ProducerRecord<String,String> record = new ProducerRecord<>("firsttopic",key,"kafka from java"+Integer.toString(i));
            logger.info("\nKey: "+key+"\n");
            // key-partition 10202202122121202211
            //send kafka data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //this function runs when record is sent or an exception is thrown
                    if(e!=null){
                        // error occures
                        logger.error("error while producing",e);
                    }else {
                        // record sent
                        logger.info("received new meta data \n"+
                                "Topic: "+recordMetadata.topic()+
                                "\nPartition: "+recordMetadata.partition()+
                                "\nOffset: "+ recordMetadata.offset()+
                                "\nTimeStamp: "+recordMetadata.timestamp());
                    }
                }
            }).get(); // blocks the .send to make it synchronous - not recommended method
        }
        producer.flush();
        producer.close();

    }
}
