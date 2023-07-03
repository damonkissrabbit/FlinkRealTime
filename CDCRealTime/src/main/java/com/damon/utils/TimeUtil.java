package com.damon.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class TimeUtil {

    public static Long getTime(String time) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.parse(time).getTime();
    }
}
