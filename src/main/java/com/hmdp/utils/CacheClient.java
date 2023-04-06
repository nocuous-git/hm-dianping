package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import com.hmdp.entity.Shop;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.sql.Time;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Component
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key,"1",LOCK_SHOP_TTL,TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private Boolean unlock(String key){
        return stringRedisTemplate.delete(key);
    }

    @SneakyThrows
    public <R,ID> void saveShopToRedis(String keyPrefix,ID id, long expireSeconds, Function<ID,R> dbFallback){
        // 1.查询店铺数据
        R r = dbFallback.apply(id);
        // 2.封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(r);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        // 3.写入Redis
        stringRedisTemplate.opsForValue().set(keyPrefix + id,JSONUtil.toJsonStr(redisData));
    }

    public CacheClient(StringRedisTemplate stringRedisTemplate){
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit){
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(redisData));
    }

    public <R,ID> R queryWithLogicalExpire(
            String keyPrefix,String lockPrefix, ID id,
            Class<R> type, Long time,
            TimeUnit unit,Long nullTTL, TimeUnit ttlUnit,
            Function<ID, R> dbFallback){
        // 1.从redis查询商铺缓存
        String key = keyPrefix + id;
        String Json = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在
        if(StringUtils.isBlank(Json)){
            if("".equals(Json)){
                return null;
            }
            // 3.不存在，查询数据库
            R r = dbFallback.apply(id);
            if(r == null){
                this.set(key, "", nullTTL, ttlUnit);
                return null;
            }
            this.setWithLogicalExpire(key, r, time, unit);
            return r;
        }
        // 4.命中，需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(Json,RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(),type);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 5.判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            // 5.1 未过期，直接返回店铺信息
            return r;
        }
        // 5.2 已过期，需要缓存重建
        // 6. 缓存重建
        // 6.1 获取互斥锁
        String lockKey = lockPrefix + id;
        boolean isLock = tryLock(lockKey);
        // 6.2 判断是否获取锁成功
        if(isLock){
            // 6.3 成功，开启独立线程，实现缓存重建
            // 重新查询缓存防止无效更新
            Json = stringRedisTemplate.opsForValue().get(key);
            redisData = JSONUtil.toBean(Json, RedisData.class);
            if(redisData.getExpireTime().isAfter(LocalDateTime.now())){
                r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
                return r;
            }
            // 重新获取后未过期则开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try {
                    R newValue = dbFallback.apply(id);
                    this.setWithLogicalExpire(key,newValue,time,unit);
                } catch (RuntimeException e) {
                    throw new RuntimeException();
                }finally {
                    unlock(lockKey);
                }
            });
        }
        // 6.4 返回过期的商铺信息
        return r;
    }
}
