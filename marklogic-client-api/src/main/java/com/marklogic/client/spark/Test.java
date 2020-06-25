package com.marklogic.client.spark;

import java.sql.Date;
import java.text.SimpleDateFormat;

public class Test {
    public static void main(String args[]) {

    System.out.println("************ Hello World ****************");
    System.out.println("************ Hello Anu **************** ");

        SimpleDateFormat formatter= new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss z");
        Date date = new Date(System.currentTimeMillis());
        System.out.println(formatter.format(date));
    }
}
