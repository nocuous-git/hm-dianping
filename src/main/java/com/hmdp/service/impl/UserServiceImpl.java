package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Autowired
    private JavaMailSender mailSender;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result sendCode(String phone, HttpSession session) {
        //1.效验手机号
        if(RegexUtils.isPhoneInvalid(phone)){
            //2.如果不符合
            return Result.fail("手机号格式错误！");
        }
        //3.符合，生成验证码
        String code = RandomUtil.randomNumbers(6);
       /* 基于session实现短信登录功能
          4.保存验证码到session
        session.setAttribute("code",code);
        */
        /*基于Redis实现短信登录功能*/
        // 4.保存验证码到Redis
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone,code,LOGIN_CODE_TTL, TimeUnit.MINUTES);
        //5.发送验证码
        log.debug("发送短信验证码成功,验证码:{}", code);
        // 返回ok
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        //1.效验手机号
        String phone = loginForm.getPhone();
        if(RegexUtils.isPhoneInvalid(phone)){
            //2.不符合，返回错误信息
            return Result.fail("手机号格式错误!");
        }
        /*
        3.基于session校验验证码
        Object cacheCode = session.getAttribute("code");
        */
        //TODO 3.从redis中获取验证码并效验
        String cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
        String code = loginForm.getCode();
        if (cacheCode == null || !cacheCode.toString().equals(code)) {
            //3.不一致，报错
            return Result.fail("验证码错误！");
        }
        //4.一致，根据手机号查询用户
        User user = query().eq("phone",phone).one();
        //5.判断用户是否存在
        if (user == null) {
            //6.不存在，创建新用户
            user = creatUserWithPhone(phone);
        }
        /*
        7.保存用户信息到session中
        session.setAttribute("user", BeanUtil.copyProperties(user, UserDTO.class));
        */
        // 7、 保存用户信息到Redis中
        // TODO 7.1、随机生成Token，作为令牌登录
        String token = UUID.randomUUID().toString(true);
        // TODO 7.2、将User对象转为Hash存储
        UserDTO userDTO = BeanUtil.copyProperties(user,UserDTO.class);
        Map<String,Object> usermap = BeanUtil.beanToMap(userDTO,new HashMap<>(),
                CopyOptions.create()
                        .setIgnoreNullValue(true)
                        .setFieldValueEditor((fieldName,fieldValue)-> fieldValue.toString()));
        // TODO 7.3、存储
        String tokenkey = LOGIN_USER_KEY + token;
        stringRedisTemplate.opsForHash().putAll(tokenkey,usermap);
        // 7.4、设置token有效期
        stringRedisTemplate.expire(tokenkey,LOGIN_USER_TTL,TimeUnit.MINUTES);
        //返回token
        return Result.ok(token);
    }

    @Override
    public Result sign() {
        // 获取当前用户
        UserDTO user = UserHolder.getUser();
        // 获取日期
        LocalDateTime now = LocalDateTime.now();
        // 拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + user.getId() + keySuffix;
        // 获取今天是本月的第几天
        int dayOfMonth = now.getDayOfMonth();
        // 写入Redis setbit key offset 1
        stringRedisTemplate.opsForValue().setBit(key, dayOfMonth -1 , true);
        return Result.ok("签到成功");
    }

    @Override
    public Result signCount() {
        // 1. 获取当前登录用户
        UserDTO user = UserHolder.getUser();
        // 判断是否登录
        if(user == null){
            return Result.fail("请先登录");
        }
        // 2. 获取日期
        LocalDateTime time = LocalDateTime.now();
        // 3. 拼接key
        String keySuffix = time.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + user.getId() + keySuffix;
        // 4. 获取今天是本月的第几天
        int dayOfMonth = time.getDayOfMonth();
        // 5. 获取本月截止今天为止的所有签到记录，返回的是一个十进制的数字 BITFIELD sign:5:202302 GET u14 0
        List<Long> result = stringRedisTemplate.opsForValue().bitField(
                key,
                BitFieldSubCommands.create().
                        get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth)).valueAt(0)
        );
        if ( result == null || result.isEmpty()){
            return Result.ok(0);
        }
        Long num = result.get(0);
        if(num == null || num == 0) {
            return Result.ok(0);
        }
        int count = 0;
        // 6. 循环遍历
        while((num & 1) != 0) {
            count++;
            num >>>= 1;
        }
        return Result.ok(count);
    }

    private User creatUserWithPhone(String phone) {
        //创建用户
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX+RandomUtil.randomString(10));
        save(user);
        return user;
    }
}
