package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.alibaba.fastjson.JSONObject;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import top.jfunc.json.JsonObject;

import javax.annotation.Resource;
import java.util.List;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_LIST_KEY;

@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    StringRedisTemplate redis;
    @Override
    public Result queryList() {
//        1.去redis查询商铺列表
        String cacheShopList = redis.opsForValue().get(CACHE_SHOP_LIST_KEY);
//        2.redis中有->直接返回
        if (StrUtil.isNotBlank(cacheShopList)){
            //转化为数组对象
            List<ShopType> shopTypes = JSONObject.parseArray(cacheShopList, ShopType.class);
            return Result.ok(shopTypes);
        }
//        3.redis中没有->去数据库中查询
        List<ShopType> shopList = query().orderByAsc("sort").list();
//        4.数据库中没有->返回失败
        if (shopList.isEmpty()){
            return Result.fail("暂无数据");
        }
//        5.数据库中有->存入redis并且返回
        redis.opsForValue().set(CACHE_SHOP_LIST_KEY, JSONUtil.toJsonStr(shopList));

        return Result.ok(shopList);
    }
}
