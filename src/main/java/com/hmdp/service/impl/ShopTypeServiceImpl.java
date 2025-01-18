package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

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

    /**
     * 根据类型查询店铺列表
     * @return
     */
    @Override
    public Result queryTypeList() {
        // 1. 查询redis中列表缓存
        String key = RedisConstants.CACHE_TYPE_KEY;
        String shopList = stringRedisTemplate.opsForValue().get(key);
        // 2. 存在直接返回
        if (StrUtil.isNotBlank(shopList)) {
            List<ShopType> typeList = JSONUtil.toList(shopList, ShopType.class);
            return Result.ok(typeList);
        }

        // 3. 不存在则查询数据库
        List<ShopType> typeList = query().orderByAsc("sort").list();

        // 4.写入redis缓存
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(typeList));

        // 5. 返回结果
        return Result.ok(typeList);

    }
}
