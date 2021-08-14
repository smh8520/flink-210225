package com.atguigu.app.function;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author smh
 * @create 2021-08-02 9:55
 */
public class Test {
    public static void main(String[] args) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date parse = simpleDateFormat.parse("2021-08-02 09:56:30");
        System.out.println(parse);
    }
}
