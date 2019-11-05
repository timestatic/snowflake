package com.timestatic.snowflake;

/**
 * @Author: timestatic
 * @Date: 2019/11/5 20:39
 */
public class Sample {

    public static void main(String[] args) {
        ZookeeperWorker.init("127.0.0.1");
        System.out.println(SnowFlakeWorker.getInstance().nextId());
        System.out.println(SnowFlakeWorker.getInstance().nextId());
    }
}
