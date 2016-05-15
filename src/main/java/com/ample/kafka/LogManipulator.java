package com.ample.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

/**
 * Created by Dhwanit on 13/05/16.
 */
public class LogManipulator {

    public LogManipulator(){

    }

    public void addRecords(ConsumerRecords<String, String> consumerRecords) {

        for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {

            try {

                Map<String, String> recordMap = KafkaMapUtils.convertStringToHashMap(consumerRecord.value());

                LogWriter.appendRecord(recordMap, consumerRecord.value());

            } catch (Exception e) {
                System.out.println("Failed to log " + consumerRecord.value());
                System.out.println("Exception " + e.getMessage());
            }
        }
    }
}
