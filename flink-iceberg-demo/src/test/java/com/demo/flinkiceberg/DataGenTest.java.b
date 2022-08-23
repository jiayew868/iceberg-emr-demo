package com.demo.flinkiceberg;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

import static org.junit.Assert.*;

public class DataGenTest {

    @org.junit.Test
    public void genObj() {

        String random11 = RandomStringUtils.random(15, true, false);
        System.out.println(random11);


        for (int i = 0; i < 10; i++) {
            System.out.println(RandomUtils.nextInt(1,6));
            System.out.println(Date.from(LocalDateTime.now().atZone(ZoneId.of("Asia/Shanghai")).toInstant()));
        }

    }

    @org.junit.Test
    public void genObj2() {
        System.out.println(System.currentTimeMillis());


        int b = 10 % 10;

        int b2 = 100 % 10;

        int b3 = 1000 % 10;

        System.out.println(b);
        System.out.println(b2);
        System.out.println(b3);

        for (int i = 0; i < 10; i++) {
            System.out.println(DataGen.genClickEventObj());
        }

    }
}