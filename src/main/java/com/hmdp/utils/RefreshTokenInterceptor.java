package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.UserDTO;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.LOGIN_USER_KEY;
import static com.hmdp.utils.RedisConstants.LOGIN_USER_TTL;

public class RefreshTokenInterceptor implements HandlerInterceptor {
    private StringRedisTemplate redis;

    public RefreshTokenInterceptor(StringRedisTemplate redis) {
        this.redis = redis;
    }
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        //1.获取正在登录的用户信息
        //1.1获取请求头中的token
        String token = request.getHeader("authorization");
        if (StrUtil.isBlank(token)){
            return true;
        }
        //2.基于token获取缓存中的用户
        String key = LOGIN_USER_KEY + token;
        Map<Object, Object> userMap = redis.opsForHash().entries(key);

        //3.判断用户是否存在
        if (userMap.isEmpty()){
            return true;
        }
        //4.将查询到的Hash数据转化为UserDto对象，保存到threadLocal中。为了线程安全
        UserDTO userDto = BeanUtil.fillBeanWithMap(userMap, new UserDTO(), false);

        //5.保存用户信息到threadLocal，
        UserHolder.saveUser(userDto);
        //6.刷新token有效期
        redis.expire(key,LOGIN_USER_TTL, TimeUnit.MINUTES);
        //7.放行
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        HandlerInterceptor.super.afterCompletion(request, response, handler, ex);
    }
}
