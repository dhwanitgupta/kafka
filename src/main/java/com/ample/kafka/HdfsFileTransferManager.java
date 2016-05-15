package com.ample.kafka;

import org.apache.commons.lang.ObjectUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.text.ParseException;
import java.util.*;

/**
 * Created by Dhwanit on 15/05/16.
 */
public final class HdfsFileTransferManager {

    private static String DATE_FORMAT = "dd-MM-yyyy-HH";

    public static void transferLocalFileToHdfs(ConsumerRecords<String, String> consumerRecords) {

        if (consumerRecords == null || consumerRecords.count() == 0) {
            return;
        }

        Date minDate = getMinimumDate(consumerRecords);
        List<String> filePaths = getFilesWithTimeStampLessThanMinDate(minDate);
        for (String path : filePaths) {
            HdfsFileUtils.moveFileToHdfs(path);
            //System.out.println("Path " + path);
        }
    }

    private static List<String> getFilesWithTimeStampLessThanMinDate(Date minDate) {

        Map<String, Date> pathDateMap = FileUtil.getFilesWithTimeStampLessThanMinDate(minDate);
        List<String> filePaths = new ArrayList<String>();

        for (Map.Entry<String, Date> entry : pathDateMap.entrySet())
        {
            filePaths.add(entry.getKey());
        }

        return filePaths;
    }

    private static Date getMinimumDate(ConsumerRecords<String, String> consumerRecords) {

        Date minDate = null;

        for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {

            Map<String, String> recordMap = KafkaMapUtils.convertStringToHashMap(consumerRecord.value());
            String formatedDate = DateTimeUtil.getFormattedDateTime(recordMap.get("timestamp"), DATE_FORMAT);

            try {
                if (minDate == null) {
                    minDate = DateTimeUtil.getDateFromFormattedString(formatedDate, DATE_FORMAT);
                } else {
                    Date date = DateTimeUtil.getDateFromFormattedString(formatedDate, DATE_FORMAT);
                    if (date.compareTo(minDate) < 0) {
                        minDate = date;
                    }
                }
            }catch (ParseException e){}
        }


        return minDate;
    }
}
