package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.ProductStats;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

/**
 * Author: Felix
 * Date: 2021/2/23
 * Desc: 商品主题统计应用
 */
public class ProductStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 创建Flink流式处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        /*
        //1.3 检查点CK相关设置
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend(
                "hdfs://hadoop202:8020/gmall/flink/checkpoint/ProductStatsApp");
        env.setStateBackend(fsStateBackend);
        System.setProperty("HADOOP_USER_NAME","atguigu");
        */

        //TODO 2.从Kafka中获取数据流
        //2.1 声明相关的主题名称以及消费者组
        String groupId = "product_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        String favorInfoSourceTopic = "dwd_favor_info";
        String cartInfoSourceTopic = "dwd_cart_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";

        //2.2 从页面日志中获取点击和曝光数据
        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId);
        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);

        //2.3 从dwd_favor_info中获取收藏数据
        FlinkKafkaConsumer<String> favorInfoSourceSouce = MyKafkaUtil.getKafkaSource(favorInfoSourceTopic, groupId);
        DataStreamSource<String> favorInfoDStream = env.addSource(favorInfoSourceSouce);

        //2.4 从dwd_cart_info中获取购物车数据
        FlinkKafkaConsumer<String> cartInfoSource = MyKafkaUtil.getKafkaSource(cartInfoSourceTopic, groupId);
        DataStreamSource<String> cartInfoDStream = env.addSource(cartInfoSource);

        //2.5 从dwm_order_wide中获取订单数据
        FlinkKafkaConsumer<String> orderWideSource = MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId);
        DataStreamSource<String> orderWideDStream = env.addSource(orderWideSource);

        //2.6 从dwm_payment_wide中获取支付数据
        FlinkKafkaConsumer<String> paymentWideSource = MyKafkaUtil.getKafkaSource(paymentWideSourceTopic, groupId);
        DataStreamSource<String> paymentWideDStream = env.addSource(paymentWideSource);

        //2.7 从dwd_order_refund_info中获取退款数据
        FlinkKafkaConsumer<String> refundInfoSource = MyKafkaUtil.getKafkaSource(refundInfoSourceTopic, groupId);
        DataStreamSource<String> refundInfoDStream = env.addSource(refundInfoSource);

        //2.8 从dwd_order_refund_info中获取评价数据
        FlinkKafkaConsumer<String> commentInfoSource = MyKafkaUtil.getKafkaSource(commentInfoSourceTopic, groupId);
        DataStreamSource<String> commentInfoDStream = env.addSource(commentInfoSource);


        //TODO 3.将各个流的数据转换为统一的对象格式
        //3.1 对点击和曝光数据进行转换      jsonStr-->ProduceStats
        SingleOutputStreamOperator<ProductStats> productClickAndDispalyDS = pageViewDStream.process(
            new ProcessFunction<String, ProductStats>() {
                @Override
                public void processElement(String jsonStr, Context ctx, Collector<ProductStats> out) throws Exception {
                    //将json格式字符串转换为json对象
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                    String pageId = pageJsonObj.getString("page_id");
                    if (pageId == null) {
                        System.out.println(">>>>" + jsonObj);
                    }
                    //获取操作时间
                    Long ts = jsonObj.getLong("ts");
                    //如果当前访问的页面是商品详情页，认为该商品被点击了一次
                    if ("good_detail".equals(pageId)) {
                        //获取被点击商品的id
                        Long skuId = pageJsonObj.getLong("item");
                        //封装一次点击操作
                        ProductStats productStats = ProductStats.builder().sku_id(skuId).click_ct(1L).ts(ts).build();
                        //向下游输出
                        out.collect(productStats);
                    }

                    JSONArray displays = jsonObj.getJSONArray("displays");
                    //如果displays属性不为空，那么说明有曝光数据
                    if (displays != null && displays.size() > 0) {
                        for (int i = 0; i < displays.size(); i++) {
                            //获取曝光数据
                            JSONObject displayJsonObj = displays.getJSONObject(i);
                            //判断是否曝光的某一个商品
                            if ("sku_id".equals(displayJsonObj.getString("item_type"))) {
                                //获取商品id
                                Long skuId = displayJsonObj.getLong("item");
                                //封装曝光商品对象
                                ProductStats productStats = ProductStats.builder().sku_id(skuId).display_ct(1L).ts(ts).build();
                                //向下游输出
                                out.collect(productStats);
                            }
                        }
                    }

                }
            }
        );

        env.execute();

    }
}
