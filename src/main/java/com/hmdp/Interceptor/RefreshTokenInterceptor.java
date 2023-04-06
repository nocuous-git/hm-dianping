package com.hmdp.Interceptor;


import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import com.hmdp.dto.UserDTO;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.LOGIN_USER_KEY;
import static com.hmdp.utils.RedisConstants.LOGIN_USER_TTL;

public class RefreshTokenInterceptor implements HandlerInterceptor {

    private StringRedisTemplate stringRedisTemplate;

    public RefreshTokenInterceptor(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }


    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        /*
        1.获取session
        HttpSession session = request.getSession();
        */
        //1.获取请求头中的token
        String token = request.getHeader("authorization");
        if(StringUtils.isBlank(token)){
            return true;
        }
        /*
        2.获取session中的用户
        Object user = session.getAttribute("user");
        */
        // 基于TOKEN获取redis中的用户
        String key = LOGIN_USER_KEY + token;
        Map<Object, Object> userMap = stringRedisTemplate.opsForHash().entries(key);
        //3.判断用户是否存在
        if(userMap.isEmpty()){
            //4.不存在，拦截，返回401状态码
            return true;
        }
        /*
        5.存在，保存用户信息到ThreadLocal
        UserHolder.saveUser((UserDTO) user);
        */
        // 5.将查询到的Hash数据转化为UserDTO对象
        UserDTO userDTO = BeanUtil.fillBeanWithMap(userMap,new UserDTO(), false);
        // 6.存在，保存用户信息到ThreadLocal
        UserHolder.saveUser(userDTO);
        // 7.刷新token有效期
        stringRedisTemplate.expire(key,LOGIN_USER_TTL, TimeUnit.MINUTES);
        return true;
    }
}
