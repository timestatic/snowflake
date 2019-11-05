package com.timestatic.snowflake;


import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;

/**
 * @Author: timestatic
 * @Date: 2019/11/2 22:32
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(value = {System.class, SnowFlakeWorker.class})
public class SnowFlakeWorkerTest {

    private SnowFlakeWorker snowFlakeWorker;
    private static long workId = 1L;

    @Before
    public void init() {
        SnowFlakeWorker.initSnowFlakeWorker(workId);
        snowFlakeWorker = SnowFlakeWorker.getInstance();
    }

    @Test
    public void initErrorTest() {
        try {
            SnowFlakeWorker.initSnowFlakeWorker(1024L);
        } catch (Exception e) {
            Assert.assertThat(e.getMessage(), is("datacenterId can't be greater than MAX_DATACENTER_NUM or less than 0"));
        }
        try {
            SnowFlakeWorker.initSnowFlakeWorker(-3L);
        } catch (Exception e) {
            Assert.assertThat(e.getMessage(), is("datacenterId can't be greater than MAX_DATACENTER_NUM or less than 0"));
        }

    }

    @Test
    public void testCreateId() throws InterruptedException {
        int count = 10;
        ConcurrentHashMap resultSet = new ConcurrentHashMap<>();
        Executor executor = Executors.newFixedThreadPool(count);
        CountDownLatch countDownLatch = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            executor.execute(() -> {
                resultSet.put(snowFlakeWorker.nextId(), 0);
                countDownLatch.countDown();
            });
        }
        countDownLatch.await();
        Assert.assertEquals(count, resultSet.size());
    }


    private final static long START_STMP = 1288834974657L;
    private final static long SEQUENCE_BIT = 12;
    private final static long WORK_BIT = 10;
    private final static long WORK_LEFT = SEQUENCE_BIT;
    private final static long TIMESTMP_LEFT = WORK_LEFT + WORK_BIT;
    private final static long STEP_SIZE = 1024L;

    @Test
    public void testClockBackwards() throws InterruptedException {
        int size = 100;
        long beginTime = System.currentTimeMillis();
        Set<Long> resultSet = new HashSet<>(128);
        for (int i = 0; i < size; i++) {
            resultSet.add(snowFlakeWorker.nextId());
        }
        TimeUnit.SECONDS.sleep(1);
        PowerMockito.mockStatic(System.class);
        // 第一次回拨
        PowerMockito.when(System.currentTimeMillis()).thenReturn(beginTime);

        Assert.assertEquals(beginTime, System.currentTimeMillis());

        for (int i = 0; i < 5; i++) {
            // i > 0, currStmp == lastStmp
            long temp = (beginTime - START_STMP) << TIMESTMP_LEFT
                    | workId << WORK_LEFT
                    | (STEP_SIZE + i);
            long tmpR;
            resultSet.add(tmpR = snowFlakeWorker.nextId());
            Assert.assertEquals(temp, tmpR);
        }
        Assert.assertEquals(size + 5, resultSet.size());

        // 下一毫秒
        PowerMockito.when(System.currentTimeMillis()).thenReturn(beginTime += 1);
        long temp = (beginTime - START_STMP) << TIMESTMP_LEFT | workId << WORK_LEFT | STEP_SIZE;
        Assert.assertEquals(temp, snowFlakeWorker.nextId());
        // 同一毫秒内
        temp = (beginTime - START_STMP) << TIMESTMP_LEFT | workId << WORK_LEFT | (STEP_SIZE + 1);
        Assert.assertEquals(temp, snowFlakeWorker.nextId());
        temp = (beginTime - START_STMP) << TIMESTMP_LEFT | workId << WORK_LEFT | (STEP_SIZE + 2);
        Assert.assertEquals(temp, snowFlakeWorker.nextId());

        // 第二次回拨
        PowerMockito.when(System.currentTimeMillis()).thenReturn(beginTime -= 5);
        temp = (beginTime - START_STMP) << TIMESTMP_LEFT | workId << WORK_LEFT | (STEP_SIZE * 2);
        Assert.assertEquals(temp, snowFlakeWorker.nextId());
        // 不同毫秒内
        PowerMockito.when(System.currentTimeMillis()).thenReturn(beginTime += 1);
        temp = (beginTime - START_STMP) << TIMESTMP_LEFT | workId << WORK_LEFT | (STEP_SIZE * 2);
        Assert.assertEquals(temp, snowFlakeWorker.nextId());

        // 第三次回拨
        PowerMockito.when(System.currentTimeMillis()).thenReturn(beginTime -= 10);
        temp = (beginTime - START_STMP) << TIMESTMP_LEFT | workId << WORK_LEFT | (STEP_SIZE * 3);
        Assert.assertEquals(temp, snowFlakeWorker.nextId());
        // 同一毫秒内
        temp = (beginTime - START_STMP) << TIMESTMP_LEFT | workId << WORK_LEFT | (STEP_SIZE * 3 + 1);
        Assert.assertEquals(temp, snowFlakeWorker.nextId());
        temp = (beginTime - START_STMP) << TIMESTMP_LEFT | workId << WORK_LEFT | (STEP_SIZE * 3 + 2);
        Assert.assertEquals(temp, snowFlakeWorker.nextId());
    }

}