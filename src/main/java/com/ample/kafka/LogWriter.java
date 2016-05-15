package com.ample.kafka;

import java.io.*;
import java.util.Map;

/**
 * Created by Dhwanit on 15/05/16.
 */
public final class LogWriter {

    public static void appendRecord(Map<String, String> recordMap, String record) {

        String database = recordMap.get("database");
        String table = recordMap.get("table");
        String timeStamp = DateTimeUtil.getFormattedDateTime(recordMap.get("timestamp"), "dd-MM-yyyy-HH");

//        String logFileName = FileUtil.getLogFileName(timeStamp);

        String filePath = FileUtil.getFilePath(database, table, timeStamp);

        appendRecordInFile(filePath, recordMap, record);
    }

    public static void appendRecordInFile(String filePath, Map<String, String> recordMap, String record) {

        StringBuilder sb = new StringBuilder();
        sb.append(recordMap.get("timestamp")).append("::").append(record).append("\n");

        try {
            BufferedWriter out = new BufferedWriter(new FileWriter(filePath, true));
            out.write(sb.toString());
            out.close();

        } catch (IOException e) {
            System.out.println("exception occoured "+ e);
        }
    }
}
