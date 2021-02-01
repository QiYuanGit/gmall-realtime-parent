package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Author: Felix
 * Date: 2021/2/1
 * Desc:  配置表处理函数
 */
public class TableProcessFunction extends ProcessFunction<JSONObject,JSONObject> {
    private OutputTag<JSONObject> outputTag;

    public TableProcessFunction(OutputTag<JSONObject> outputTag) {
        this.outputTag = outputTag;
    }

    @Override
    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {

    }
}
