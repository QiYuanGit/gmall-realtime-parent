package com.atguigu.gmall.controller;

import com.atguigu.gmall.service.ProductStatsService;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.Date;

/**
 * Author: Felix
 * Date: 2021/2/26
 * Desc: 大屏展示的控制层
 * 主要职责：接收客户端的请求(request)，对请求进行处理，并给客户端响应(response)
 *
 * @RestController = @Controller + @ResponseBody
 * @RequestMapping()可以加在类和方法上 加在类上，就相当于指定了访问路径的命名空间
 *
 */
@RestController
@RequestMapping("/api/sugar")
public class SugarController {

    //将service注入进来
    @Autowired
    ProductStatsService productStatsService;

    /**
     * 请求路径： /api/sugar/gmv
     * 返回值类型：
     * {
     * "status": 0,
     * "msg": "",
     * "data": 1201076.1961842624
     * }
     */
    @RequestMapping("/gmv")
    public String getGMV(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            date = now();
        }
        BigDecimal gmv = productStatsService.getGMV(date);
        String json = "{" +
            "\"status\": 0," +
            "\"data\": " + gmv +
            "}";
        return json;
    }

    private Integer now() {
        String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
        return Integer.valueOf(yyyyMMdd);
    }

}
