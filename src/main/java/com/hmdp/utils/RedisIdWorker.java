package com.hmdp.utils;

import lombok.SneakyThrows;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class RedisIdWorker {

    private static final long BEGIN_TIMESTAMP = 1640995200L;

    private static final short COUNT_BITS = 32;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    // Test
    private static ExecutorService es = Executors.newFixedThreadPool(500);

    public long nextId(String keyPrefix){
        // 1.生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        long timestamp = nowSecond - BEGIN_TIMESTAMP;
        // 2. 生成序列号
        // 2.1 获取当前日期，精确到天
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        // 2.2 自增长
        long count = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + date);
        // 3.拼接并返回
        return timestamp << COUNT_BITS | count;
    }

    /*@SneakyThrows
    public static void main(String[] args) {
        CountDownLatch latch = new CountDownLatch(300);

        Runnable task = () -> {
          for(int i = 0; i < 100; i++){
              long id = nextId("order");
              System.out.println("id = " + id);
          }
          latch.countDown();
        };
        long begin = System.currentTimeMillis();
        for(int i = 0; i < 300; i++){
            es.submit(task);
        }
        latch.await();
        long end = System.currentTimeMillis();
        System.out.println("time = " + (end - begin));
    }*/

}
