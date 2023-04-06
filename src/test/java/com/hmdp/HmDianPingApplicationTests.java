package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import com.hmdp.service.impl.ShopServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.SHOP_GEO_KEY;

@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest(classes = HmDianPingApplication.class)
class HmDianPingApplicationTests {

/*    @Autowired
    private ShopServiceImpl shopService;*/

    @Resource
    private RedissonClient redissonClient;

    @Resource
    private RedissonClient redissonClient2;

    @Resource
    private RedissonClient redissonClient3;


    @Autowired
    private IShopService shopService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private RLock rLock;

    @BeforeEach
    void setUp(){
        RLock lock1 = redissonClient.getLock("order");
        RLock lock2 = redissonClient2.getLock("order");
        RLock lock3 = redissonClient3.getLock("order");

        //创建联锁 multiLock
        rLock = redissonClient.getMultiLock(lock1,lock2,lock3);
    }

    @Test
    void method1() throws InterruptedException {
        //尝试获取锁
        boolean isLock = rLock.tryLock(1L, TimeUnit.SECONDS);
        if (!isLock){
            log.error("获取锁失败 ... 1");
            return ;
        }
        try {
            log.info("获取锁成功 ... 1");
            method2();
            log.info("开始执行业务 ... 1");
        }finally {
            log.warn("准备释放锁 ... 1");
            rLock.unlock();
        }
    }

    void method2(){
        //尝试获取锁
        boolean isLock = rLock.tryLock();
        if (!isLock){
            log.error("获取锁失败 ... 2");
            return ;
        }
        try {
            log.info("获取锁成功 ... 2");
            log.info("开始执行业务 ... 2");
        }finally {
            log.warn("准备释放锁 ... 2");
            rLock.unlock();
        }
    }

    @Test
    void loadShopData(){
        // 1.查询店铺信息
        List<Shop> list = shopService.list();
        // 2.把店铺分组，按照typeId分组，typeId一致的放到一个集合
        Map<Long, List<Shop>> map = list.stream().collect(Collectors.groupingBy(Shop::getTypeId));
        // 3.分批完成写入Redis
        for(Map.Entry<Long, List<Shop>> entry : map.entrySet()){
            // 3.1获取类型id
            Long typeId = entry.getKey();
            String key = SHOP_GEO_KEY + typeId;
            // 3.2 获取同类型的店铺的集合
            List<Shop> value = entry.getValue();
            List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>(value.size());
            // 3.3 写入redis GEOADD key 经度 纬度 member
            for(Shop shop : value){
                locations.add(new RedisGeoCommands.GeoLocation<>(
                        shop.getId().toString(),
                        new Point(shop.getX(), shop.getY())
                ));
            }
            stringRedisTemplate.opsForGeo().add(key,locations);
        }
    }

    @Test
    void testHyperLogLog(){
        String[] values = new String[1000];
        int j = 0;
        for(int i = 0; i < 1000000; i++){
            j = i %1000;
            values[j] = "user_" + i;
            if(j == 999){
                stringRedisTemplate.opsForHyperLogLog().add("hl2", values);
            }
        }
        Long count = stringRedisTemplate.opsForHyperLogLog().size("hl2");
        System.out.println("count = " + count);
    }

}
