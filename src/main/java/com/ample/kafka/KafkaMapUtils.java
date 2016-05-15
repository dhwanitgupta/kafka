package com.ample.kafka;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Dhwanit on 13/05/16.
 */
public final class KafkaMapUtils {

    public static Map<String, String> convertStringToHashMap(String record){

        Map<String, String> recordMap = new HashMap<String, String>();

        record = record.trim();

        String [] keyValuePairs = record.split("##");

        for (String pair : keyValuePairs) {

            String[] entry = pair.split("=>", 2);

            recordMap.put(entry[0].trim(), entry[1].trim());
        }

        return  recordMap;
    }
}
