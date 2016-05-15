package com.ample.kafka;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by Dhwanit on 13/05/16.
 */
public final class DateTimeUtil {

    public static String getFormattedDateTime(String epoch, String dateFormat) {

        Long epochInLong = Long.parseLong(epoch);
        DateFormat format = new SimpleDateFormat(dateFormat);

        format.setTimeZone(TimeZone.getTimeZone("IST"));

        String formattedDate = format.format(epochInLong);

        return formattedDate;
    }

    public static Date getDateFromEpoch(String epoch) {
        Long epochInLong = Long.parseLong(epoch);
        return new Date(epochInLong);
    }

    public static Date getDateFromFormattedString(String formattedDate, String dateFormat) throws ParseException {
        DateFormat format = new SimpleDateFormat(dateFormat);
        try {
            Date date = format.parse(formattedDate);
            return date;
        } catch (ParseException e) {
            System.out.println("Not able to parse date " + formattedDate + " EX: " + e.getMessage());
            throw e;
        }
    }
}
