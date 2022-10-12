package com.kafka.java;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import java.util.Properties;

public class ProduerDemoWithCallback {
    public static void main(String[] args) {
        System.out.println("hello world");
        //create logger for this class
        Logger logger = LoggerFactory.getLogger(ProduerDemoWithCallback.class);
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
            ProducerRecord<String,String> record = new ProducerRecord<>("firsttopic","kafka from java"+Integer.toString(i));

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
            });
        }
        producer.flush();
        producer.close();

    }
}
