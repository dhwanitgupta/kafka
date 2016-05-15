package com.ample;


import com.ample.kafka.HdfsFileTransferManager;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.Arrays;
import java.util.Properties;


import com.ample.kafka.KafkaProperties;
import  com.ample.kafka.LogManipulator;

/**
 * Created by Dhwanit on 13/05/16.
 */
public class AmpleKafkaConsumer {

    public static int POLLING_TIMEOUT = 100;
    public static int SLEEPING_TIMEOUT = 10;
    public final static String GROUP_ID = "model_update";

    public static void main(String[] args) {


        KafkaProperties kafkaProperties = new KafkaProperties();

        Properties properties = kafkaProperties.getProperties();

        KafkaConsumer<String, String> consumer = new KafkaConsumer(properties);

        consumer.subscribe(Arrays.asList(GROUP_ID));

        LogManipulator logManipulator = new LogManipulator();

        while (true) {

            ConsumerRecords<String, String> records = consumer.poll(POLLING_TIMEOUT);
            logManipulator.addRecords(records);
            HdfsFileTransferManager.transferLocalFileToHdfs(records);
            try {
                Thread.sleep(SLEEPING_TIMEOUT);
            } catch(InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
