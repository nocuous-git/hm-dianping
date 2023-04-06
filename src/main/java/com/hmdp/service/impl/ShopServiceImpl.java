package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import lombok.SneakyThrows;
import org.springframework.boot.autoconfigure.cache.CacheProperties;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        // 缓存穿透
        //Shop shop = queryWithPassThrough(id);

        // 互斥锁解决缓存击穿
        //Shop shop = queryWithMutex(id);

        // 逻辑过期解决缓存击穿
        //Shop shop = queryWithLogicalExpire(id);

        // 使用编写的工具类解决穿透和击穿
        Shop shop = cacheClient.queryWithLogicalExpire(
                CACHE_SHOP_KEY, LOCK_SHOP_KEY, id,
                Shop.class, 10L, TimeUnit.SECONDS,
                30L, TimeUnit.MINUTES,
                this::getById);

        if(shop == null){
            return Result.fail("店铺不存在！");
        }
        return Result.ok(shop);
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    /**
     * @param id
     * @return
     * 缓存击穿逻辑解决  并且加入了缓存穿透空对象解决方案
     */
    public Shop queryWithLogicalExpire(Long id){
        // 1.从redis查询商铺缓存
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在
        if(StringUtils.isBlank(shopJson)){
            if("".equals(shopJson)){
                return null;
            }
            // 3.不存在，查询数据库
            Shop shop = getById(id);
            if(shop == null){
                stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
                return null;
            }
            RedisData redisData = new RedisData();
            redisData.setData(shop);
            redisData.setExpireTime(LocalDateTime.now().plusSeconds(10));
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,JSONUtil.toJsonStr(redisData));
            return shop;
        }
        // 4.命中，需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(shopJson,RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(),Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 5.判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            // 5.1 未过期，直接返回店铺信息
            return shop;
        }
        // 5.2 已过期，需要缓存重建
        // 6. 缓存重建
        // 6.1 获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        // 6.2 判断是否获取锁成功
        if(isLock){
            // 6.3 成功，开启独立线程，实现缓存重建
            // 重新查询缓存防止无效更新
            shopJson = stringRedisTemplate.opsForValue().get(key);
            redisData = JSONUtil.toBean(shopJson, RedisData.class);
            if(redisData.getExpireTime().isAfter(LocalDateTime.now())){
                return shop;
            }
            // 重新获取后未过期则开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try {
                    this.saveShopToRedis(id,10L);
                } catch (RuntimeException e) {
                    throw new RuntimeException();
                }finally {
                    unlock(lockKey);
                }
            });
        }
        // 6.4 返回过期的商铺信息
        return shop;
    }

    public Shop queryWithMutex(Long id){
        // 1.从redis查询商铺缓存
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在
        if(StringUtils.isNotBlank(shopJson)){
            // 3.存在，直接返回
            return JSONUtil.toBean(shopJson,Shop.class);
        }
        // 判断命中的是否是空值
        if(shopJson != null){
            // 返回一个错误信息
            return null;
        }
        // 4. 实现缓存重建
        // 4.1 获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            boolean islock = tryLock(lockKey);
            // 4.2 判断是否获取成功
            if(!islock){
                // 4.3 失败，则休眠并重试
                Thread.sleep(LOCK_SHOP_TTL);
                return queryWithMutex(id);
            }
            // 4.4 成功，根据id查询数据库
            shop = getById(id);
            // 模拟重建延迟
            Thread.sleep(200);
            // 5.不存在，返回错误
            if (shop == null){
                /* Todo 解决缓存穿透问题*/
                //将空值写入redis
                stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
                //返回错误信息
                return null;
            }
            // 6.存在，写入redis
            stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally{
            //7. 释放互斥锁
            unlock(lockKey);
        }
        //8. 返回
        return shop;
    }

    public Shop queryWithPassThrough(Long id){
        // 1.从redis查询商铺缓存
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在
        if(StringUtils.isNotBlank(shopJson)){
            // 3.存在，直接返回
            return JSONUtil.toBean(shopJson,Shop.class);
        }
        // 判断命中的是否是空值
        if(shopJson != null){
            // 返回一个错误信息
            return null;
        }
        // 4.不存在，根据id查询数据库
        Shop shop = getById(id);
        // 5.不存在，返回错误
        if (shop == null){
            /* Todo 解决缓存穿透问题*/
            //将空值写入redis
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
            //返回错误信息
            return null;
        }
        // 6.存在，写入redis
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //7. 返回
        return shop;
    }

    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key,"1",LOCK_SHOP_TTL,TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private Boolean unlock(String key){
        return stringRedisTemplate.delete(key);
    }

    @SneakyThrows
    public void saveShopToRedis(long id, long expireSeconds){
        // 1.查询店铺数据
        Shop shop = getById(id);
        Thread.sleep(200);
        // 2.封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        // 3.写入Redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,JSONUtil.toJsonStr(redisData));
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null){
            return Result.fail("店铺ID不能为空");
        }
        // 1.更新数据库
        updateById(shop);
        // 2.删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY+id);
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 1. 是否需要根据坐标查询
        if(x == null || y == null){
            // 不需要坐标查询，按数据库查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }
        // 2. 计算分页参数
        int from = (current -1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;
        // 3.查询redis、按照距离排序、分页。结果：shopId、distance
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .search(
                        key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
                );
        // 4. 解析出id
        if(results == null){
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if(list.size() <= from ){
            return Result.ok(Collections.emptyList());
        }
        // 4.1 截取from ~ end的部分
        List<Long> ids = new ArrayList<>(list.size());
        Map<String,Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result ->{
            // 4.2 获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });
        // 5. 根据id查询Shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for(Shop shop : shops){
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        // 返回
        return Result.ok(shops);
    }
}
