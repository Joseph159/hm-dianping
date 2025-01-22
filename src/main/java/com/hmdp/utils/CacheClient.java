package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import com.hmdp.service.impl.ShopServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;


@Component
public class CacheClient {

    private StringRedisTemplate stringRedisTemplate;

    // 设置线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);


    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }


    public void set(String key, Object value, Long time, TimeUnit timeUnit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, timeUnit);
    }

    // 设置逻辑过期
    public void setWithLogicExpire(String key, Object value, Long time, TimeUnit timeUnit){
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(timeUnit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }


    // 缓存空值工具类
    public <R, ID> R queryWithPassThrough(
            String keyPrefix,
            ID id,
            Class<R> type,
            Function<ID, R> dbFallback,
            Long time,
            TimeUnit timeUnit){
        // 1. 查询redis
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);

        // 2. 判断是否存在
        if (StrUtil.isNotBlank(json)) {
            // 存在直接返回
            return JSONUtil.toBean(json, type);
        }

        //  3. 判断值是否为空
        if (Objects.equals(json, "")){
            //  为空直接返回
            return null;
        }

        // 不为空查询数据库
        R r = dbFallback.apply(id);

        // 不存在写入空值
        if (r == null){
            this.set(key, "", time, timeUnit);
            return null;
        }

        // 写入redis
        this.set(key, r, time, timeUnit);

        // 返回
        return r;
    }

    // 获取锁
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.MINUTES);
        return BooleanUtil.isTrue(flag);
    }

    // 释放锁
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }

    // 逻辑过期解决缓存击穿
    public <R, ID> R queryWithLogicExpire(
            String keyPrefix,
            ID id,
            Class<R> type,
            Function<ID, R> dbFallback,
            Long time,
            TimeUnit timeUnit){
        // 1. 查询缓存
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);

        // 2. 判断是否存在
        if (StrUtil.isBlank(json)) {
            // 不存在直接返回
            return null;
        }

        // 3. 存在进行序列化
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        JSONObject jsonObject = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(jsonObject, type);
        LocalDateTime expireTime = redisData.getExpireTime();

        // 4. 判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())){
            // 未过期直接返回
            return r;
        }

        // 5. 过期进行缓存重建
        String lockKey = LOCK_SHOP_KEY + id;

        // 6. 缓存重建
        // 获取锁
        boolean lock = tryLock(lockKey);
        // 判断是否获取成功
        if (lock){
            // 获取成功开启线程进行缓存重建
            CACHE_REBUILD_EXECUTOR.execute(() -> {
                try {
                    // 重建缓存
                    R r1 = dbFallback.apply(id);
                    this.setWithLogicExpire(key, r1, time, timeUnit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    // 释放锁
                    unlock(lockKey);
                }
            });
        }
        // 失败直接返回
        return r;
    }
}
