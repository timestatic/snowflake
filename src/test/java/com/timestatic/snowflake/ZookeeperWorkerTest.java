package com.timestatic.snowflake;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @FileName: ZookeeperWorkerTest.java
 * @Description: ZookeeperWorkerTest.java类说明
 * @Author: jiangqian
 * @Date: 2019/11/5 19:54
 */
public class ZookeeperWorkerTest {

    @Before
    public void init() {
        ZookeeperWorker.init("127.0.0.1:2181");
    }

    @Test
    public void test() {
        Assert.assertNotNull(ZookeeperWorker.getWorkId());
        Assert.assertEquals(ZookeeperWorker.getWorkId(), ZookeeperWorker.getWorkId());
    }
}
