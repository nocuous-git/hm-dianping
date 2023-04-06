package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.util.List;
import java.util.Map;

import static com.hmdp.utils.RedisConstants.CACHE_SHOPTYPE_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryTypeList() {
        // 1.查询redis中店铺类型
        String typeJson = stringRedisTemplate.opsForValue().get(CACHE_SHOPTYPE_KEY);
        // 2.检查redis缓存中是否有店铺类型
        if(StringUtils.isNotBlank(typeJson)){
            // 3.有，直接返回
            return Result.ok(JSONUtil.toList(typeJson,ShopType.class));
        }
        // 4.没有查询数据库
        List<ShopType> typeList = list();
        // 5.判断数据库中是否有数据
        if(typeList.isEmpty()){
            return Result.fail("无类型信息");
        }
        // 6.数据库中有数据将数据库的数据放入缓存中
        typeJson = JSONUtil.toJsonStr(typeList);
        stringRedisTemplate.opsForValue().set(CACHE_SHOPTYPE_KEY,typeJson);
        // 7.返回
        return Result.ok(typeList);
    }
}
