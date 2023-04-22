package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock{
    private StringRedisTemplate redisTemplate;

    private String name;

    private static final String KET_PREFIX = "lock:";
    private static final String ID_PREFIX = UUID.randomUUID().toString()+"-";

    public SimpleRedisLock(StringRedisTemplate redisTemplate, String name) {
        this.redisTemplate = redisTemplate;
        this.name = name;
    }


    @Override
    public boolean tryLock(long timeoutSec) {
        //获取线程标识
        String threadId = ID_PREFIX+Thread.currentThread().getId();
        //获取锁
        Boolean success = redisTemplate.opsForValue().
                setIfAbsent(KET_PREFIX + name, threadId + "", timeoutSec, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(success);
    }

    @Override
    public void unlock() {
        //获取线程标识
        String threadId = ID_PREFIX + Thread.currentThread().getId();
        //获取锁中的标识
        String id = redisTemplate.opsForValue().get(KET_PREFIX+name);
        //判断是否相同
        if (threadId.equals(id)){
            //释放锁
            redisTemplate.delete(KET_PREFIX+name);
        }



    }
}
