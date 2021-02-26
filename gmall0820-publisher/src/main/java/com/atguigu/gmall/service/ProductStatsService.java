package com.atguigu.gmall.service;

import java.math.BigDecimal;

/**
 * Author: Felix
 * Date: 2021/2/26
 * Desc: 商品统计service接口
 */
public interface ProductStatsService {
    //获取某一天交易总额
    BigDecimal getGMV(int date);
}
