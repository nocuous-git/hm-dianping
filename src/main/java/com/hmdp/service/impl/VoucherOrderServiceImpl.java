package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    private static final Long SUCCESS_RETURN = 0L;

    private static final Long FAIL_RETURN = 1L;

    private IVoucherOrderService proxy;

    static{
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable{

        String queueName = "stream.orders";

        @Override
        public void run() {
            while(true){
                try {
                    // 1. 获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS streams.orders >
                    final List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    // 2. 判断消息获取是否成功
                    if(list == null || list.isEmpty()) {
                        // 2.1 如果获取失败，说明没有消息，继续下一次循环
                        continue;
                    }
                    // 3. 解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 4. 如果获取成功，可以下单
                    handleVoucherOrder(voucherOrder);
                    // 5. ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常",e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while(true){
                try {
                    // 1. 获取pending-list中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 STREAMS streams.orders 0
                    final List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    // 2. 判断消息获取是否成功
                    if(list == null || list.isEmpty()) {
                        // 2.1 如果获取失败，说明pending-list没有异常消息，结束循环
                        break;
                    }
                    // 3. 解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 4. 如果获取成功，可以下单
                    handleVoucherOrder(voucherOrder);
                    // 5. ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理pending-list订单异常",e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }

    /*private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable{

        @Override
        public void run() {
            while(true){
                try {
                    // 1. 获取队列中的订单信息
                    VoucherOrder voucherOrder = orderTasks.take();
                    handleVoucherOrder(voucherOrder);
                } catch (InterruptedException e) {
                    log.error("处理订单异常",e);
                }

            }
        }*/

        private void handleVoucherOrder(VoucherOrder voucherOrder) {
            Long userId = voucherOrder.getUserId();
            // 自己实现的锁对象
            //SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
            RLock lock =  redissonClient.getLock("lock:order:" + userId);
            if(!lock.tryLock()){
                //获取锁失败,返回错误信息
                log.error("不允许重复下单");
                return ;
            }
            // 获取代理对象(事务)
            try {
                proxy.createVoucherOrder(voucherOrder);
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        // 获取用户
        Long userId = UserHolder.getUser().getId();
        // 获取订单id
        long orderId = redisIdWorker.nextId("order");
        // 1. 执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(),
                String.valueOf(orderId)
        );
        // 2. 判断结果是否为0
        if(!SUCCESS_RETURN.equals(result)){
            // 2.1 不为0，没有购买资格
            return Result.fail(FAIL_RETURN.equals(result) ? "库存不足" : "不能重复下单");
        }
        // 3. 获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        // 3. 返回订单信息
        return Result.ok(orderId);
    }

    /*@Override
    public Result seckillVoucher(Long voucherId) {
        // 1. 执行lua脚本
        Long userId = UserHolder.getUser().getId();
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString()
                );
        // 2. 判断结果是否为0
        if(!SUCCESS_RETURN.equals(result)){
            // 2.1 不为0，没有购买资格
            return Result.fail(FAIL_RETURN.equals(result) ? "库存不足" : "不能重复下单");
        }
        // todo 2.2 为0，有资格购买，将下单信息保存到阻塞队列中
        VoucherOrder voucherOrder = new VoucherOrder();
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);
        orderTasks.add(voucherOrder);
        // 3. 获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        // 3. 返回订单信息
        return Result.ok(orderId);
    }*/

    /*
    @Override
    public Result seckillVoucher(Long voucherId) {
        // 1.查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        // 2.检查是否有优惠券
        if (voucher == null) {
            // 3.不存在返回
            return Result.fail("不存在该优惠券");
        }
        // 4.存在判断秒杀是否开始
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            // 5.尚未开始返回
            return Result.fail("秒杀尚未开始！");
        }
        // 6.判断秒杀是否已经结束
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            // 7.已经结束返回
            return Result.fail("秒杀已经结束！");
        }
        // 8.判断库存是否充足
        if (voucher.getStock() < 1) {
            return Result.fail("库存不足！");
        }
        Long userId = UserHolder.getUser().getId();
        // 自己实现的锁对象
        //SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        RLock lock =  redissonClient.getLock("lock:order:" + userId);
        if(!lock.tryLock()){
            //获取锁失败,返回错误信息
            return Result.fail("不允许重复下单");
        }
        // 获取代理对象(事务)
        try {
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            lock.unlock();
        }
    }*/

    @Override
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder){
        // 一人一单
        Long voucherId = voucherOrder.getVoucherId();
            // 查询订单
            /*int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
            // 判断是否存在
            if (count > 0) {
                log.error("不能重复购买");
                return ;
            }*/
            // 9.扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1")
                    .eq("voucher_id", voucherId).gt("stock", 0)
                    .update();
            if (!success) {
                log.error("库存空间不足");
                return ;
            }
            save(voucherOrder);
            // 11.返回订单id
            return ;
    }
}
