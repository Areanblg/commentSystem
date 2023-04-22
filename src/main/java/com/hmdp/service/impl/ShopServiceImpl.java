package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;


@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Resource
    private StringRedisTemplate redis;

    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        //缓存穿透
        Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //互斥锁解决缓存击穿
//        Shop shop = queryWithMutex(id);
        if (shop == null){
            return Result.fail("店铺不存在!");
        }
        //.从redis中查询商铺数据
        return Result.ok(shop);
    }
    private boolean tryLock(String key){
        return BooleanUtil.isTrue(redis.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS));
    }
    private void unlock(String key){
        redis.delete(key);
    }

    //解决缓存穿透
    public Shop queryWithPassThrough(Long id){
        //1.从redis中查询商铺数据
        String shoJson = redis.opsForValue().get(CACHE_SHOP_KEY + id);
        //2.查看商品数据在redis中是否存在
        if (StrUtil.isNotBlank(shoJson)){
            //存在，直接返回
            return JSONUtil.toBean(shoJson, Shop.class);
        }
        //命中的是否是空值
        if (shoJson != null){
            return null;
        }
        //3.不存在，直接查询数据库
        Shop shop = getById(id);
        if(shop == null){
            //数据库不存在数据，设置一个空值，存储到redis中，防止缓存穿透。
            redis.opsForValue().set(CACHE_SHOP_KEY+id,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }
        //4.数据库存在->存入redis
        redis.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return shop;
    }
    //解决缓存击穿
    public Shop queryWithMutex(Long id){
        //1.从redis中查询商铺数据
        String shoJson = redis.opsForValue().get(CACHE_SHOP_KEY + id);
        //2.查看商品数据在redis中是否存在
        if (StrUtil.isNotBlank(shoJson)){
            //存在，直接返回
            return JSONUtil.toBean(shoJson, Shop.class);
        }
        //命中的是否是空值
        if (shoJson != null){
            return null;
        }
        //实现缓存重建
        //1.获取互斥锁
        String lockKey = "lock:shop:"+id;
        boolean isLockKey = tryLock(lockKey);

        //2.判断是否获取成功
        if (!isLockKey){
            //3.失败，休眠并且重试
            try {
                Thread.sleep(50);
                queryWithMutex(id);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        //4.成功，根据id查询数据库
        Shop shop = getById(id);
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        if(shop == null){
            //数据库不存在数据，设置一个空值，存储到redis中，防止缓存穿透。
            redis.opsForValue().set(CACHE_SHOP_KEY+id,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }
        //5.数据库存在->存入redis
        redis.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //6.释放互斥锁
        unlock(lockKey);
        return shop;
    }

    @Override
    public Result update(Shop shop) {
        Long id = shop.getId();
        if ( id == null){
            return Result.fail("店铺id不能为空");
        }
        //1.更新数据库
        updateById(shop);
        //2.删除缓存
        redis.delete("CACHE_SHOP_KEY"+id);
        //3.返回结果
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 1.判断是否需要根据坐标查询
        if (x == null || y == null) {
            // 不需要坐标查询，按数据库查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }

        // 2.计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        // 3.查询redis、按照距离排序、分页。结果：shopId、distance
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = redis.opsForGeo() // GEOSEARCH key BYLONLAT x y BYRADIUS 10 WITHDISTANCE
                .search(
                        key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
                    );
        // 4.解析出id
        if (results == null) {
            return Result.ok();
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size() <= from) {
            // 没有下一页了，结束
            return Result.ok(Collections.emptyList());
        }
        // 4.1.截取 from ~ end的部分
        List<Long> ids = new ArrayList<>(list.size());
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            // 4.2.获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            // 4.3.获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });
        // 5.根据id查询Shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        // 6.返回
        return Result.ok(shops);
    }
}
