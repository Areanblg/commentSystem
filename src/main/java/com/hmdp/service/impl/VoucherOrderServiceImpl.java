package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import org.springframework.aop.framework.AopContext;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;

@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker idWorker;

    @Resource
    StringRedisTemplate stringRedisTemplate;
    /*
       实现用户秒杀下单
    */
    @Override
    @Transactional
    public Result seckillVoucher(Long voucherId) {
        //1.查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);

        //2.查询优惠券是否开始
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())){
            //尚未开始
            return Result.fail("秒杀尚未开始");
        }
        //3.查询优惠券是否结束
        if (voucher.getEndTime().isBefore(LocalDateTime.now())){
            return Result.fail("秒杀时间已经结束");
        }
        //4.判断库存是否充足
        if (voucher.getStock() < 1){
            //库存不足
            return Result.fail("库存不足");
        }
        Long userId = UserHolder.getUser().getId();
        //创建锁对象
        SimpleRedisLock lock = new SimpleRedisLock(stringRedisTemplate,"order:"+userId);
        //获取锁
        boolean isLock = lock.tryLock(1200);
        if (!isLock){
            //获取锁失败，直接返回错误。
            return Result.fail("不允许重复下单");
        }
        try{
            return createVoucherOrder(voucherId);
        }finally {
            lock.unlock();
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public  Result createVoucherOrder(Long voucherId) {
        //5.一人一单
        Long userId = UserHolder.getUser().getId();
        //5.1查询订单
        Integer count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        if (count > 0){
            //表示用户已经购买过了
            return Result.fail("用户已经购买过一次!");
        }
        //6.扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                .gt("stock",0)
                .update();
        if (!success){
            return Result.fail("库存不足");
        }
        //7.创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        //7.1.订单id
        long orderId = idWorker.nextId("order");
        voucherOrder.setId(orderId);
        //7.2.用户id
        voucherOrder.setUserId(userId);
        //6.3.代金券id
        voucherOrder.setVoucherId(voucherId);

        save(voucherOrder);
        //7.返回订单id
        return Result.ok(orderId);
        }



}
