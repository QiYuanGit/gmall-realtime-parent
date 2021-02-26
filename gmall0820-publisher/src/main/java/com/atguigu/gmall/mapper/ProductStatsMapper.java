package com.atguigu.gmall.mapper;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.math.BigDecimal;

/**
 * Author: Felix
 * Date: 2021/2/26
 * Desc:  商品主题统计的Mapper接口
 */
public interface ProductStatsMapper {
    //获取某一天商品的交易额
    @Select("select sum(order_amount) from product_stats_0820 where toYYYYMMDD(stt)=#{date}")
    BigDecimal getGMV(int date);
}