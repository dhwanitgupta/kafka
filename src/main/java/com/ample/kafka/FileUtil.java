package com.ample.kafka;

import java.io.File;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Dhwanit on 15/05/16.
 */
public final class FileUtil {

    public final static String BASE_DORECTORY = "/Users/Dhwanit/kafka_logs";

    public static String getLogFileName(String timeStamp) {
        StringBuilder sb = new StringBuilder();
        sb.append(timeStamp).append(".log");

        return sb.toString();
    }

    public static String getFilePath(String database, String table, String timeStamp) {

        try {
            return createPathIfNotExists(database, table, timeStamp);
        } catch (Exception e) {
            return "";
        }
    }

    public static String createPathIfNotExists(String database, String table, String timeStamp) throws Exception {

        File baseDirectory = new File(BASE_DORECTORY);
        createIfNotExists(baseDirectory);

        StringBuilder sb = new StringBuilder();
        sb.append(BASE_DORECTORY).append("/").append(database);

        createIfNotExists(new File(sb.toString()));

        sb.append("/").append(table);
        createIfNotExists(new File(sb.toString()));

        sb.append("/").append(timeStamp).append(".log");
        try {
            new File(sb.toString()).createNewFile();
            return sb.toString();
        } catch (Exception e) {
            System.out.println("Unable to create file " + sb.toString());
            e.printStackTrace();
            throw  e;
        }
    }

    public  static void createIfNotExists(File file) {
        if (!file.exists() || !file.isDirectory()) {
            Boolean isCreated = createDirectory(file);
            if (!isCreated) {
                System.out.print("Unable to create directory " + file.toString());
            }
        }
    }

    public static Boolean createDirectory(File file) {
        return file.mkdir();
    }


    public static Map<String,Date> getFilesWithTimeStampLessThanMinDate(Date minDate) {
        Map<String, Date> pathDateMap = new HashMap<String, Date>();

        File baseDirectory = new File(BASE_DORECTORY);

        getFilesWithTimeStampLessThanMinDate(BASE_DORECTORY, pathDateMap, minDate);

        return pathDateMap;
    }

    public static void getFilesWithTimeStampLessThanMinDate(String directoryName, Map<String, Date> pathDateMap, Date minDate) {
        File directory = new File(directoryName);

        // get all the files from a directory
        File[] fList = directory.listFiles();
        for (File file : fList) {
            if (file.isFile() && file.toString().endsWith(".log")) {
                String logFileName = file.getName();
                String[] splittedName = logFileName.split(".log");
                try {
                    Date fileDate = DateTimeUtil.getDateFromFormattedString(splittedName[0], "dd-MM-yyyy-HH");
                    if (fileDate.compareTo(minDate) < 0) {
                        pathDateMap.put(file.getAbsolutePath(), fileDate);
                    }
                } catch (ParseException e) {}

            } else if (file.isDirectory()) {
                getFilesWithTimeStampLessThanMinDate(file.getAbsolutePath(), pathDateMap, minDate);
            }
        }
    }
}
