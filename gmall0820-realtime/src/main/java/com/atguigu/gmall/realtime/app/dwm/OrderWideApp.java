package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DimAsyncFunction;
import com.atguigu.gmall.realtime.bean.OrderDetail;
import com.atguigu.gmall.realtime.bean.OrderInfo;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Author: Felix
 * Date: 2021/2/5
 * Desc: 合并订单宽表
 * 测试需要启动服务：
 *      -Maxwell、zk、kafka、hdfs、hbase、BaseDBApp OrderwideApp
 *      -准备配置表
 * 业务执行流程
 *      -模拟生成数据
 *      -数据插入到myql中
 *      -在Binlog中记录数据的变化
 *      -Maxwell将变化以Json的形式发送到kafka的ODS(ods_base_db_m)
 *      -BaseDBApp读取ods_base_db_m中的数据进行分流
 *          >从MySQL的配置表中读取数据
 *          >将配置缓存到map集合
 *          >检查Phoenix中的表是否存在
 *          >对数据进行分流发送到不同的dwd层主题
 *      -OrderwideApp从DWD的订单和订单明细读取数据
 */
public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1  准备本地测试流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1.2 设置并行度
        env.setParallelism(4);

        //1.3 设置Checkpoint
        //env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(60000);
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/gmall/checkpoint/uniquevisit"))

        //TODO 2.从Kafka的DWD层读取订单和订单明细数据
        //2.1 声明相关的主题以及消费者组
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";

        //2.2 读取订单主题数据
        FlinkKafkaConsumer<String> orderInfoSource = MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId);
        DataStreamSource<String> orderInfoJsonStrDS = env.addSource(orderInfoSource);

        //2.3 读取订单明细数据
        FlinkKafkaConsumer<String> orderDetailSource = MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId);
        DataStreamSource<String> orderDetailJsonStrDS = env.addSource(orderDetailSource);

        //TODO 3.对读取的数据进行结构的转换      jsonString -->OrderInfo|OrderDetail
        //3.1 转换订单数据结构
        SingleOutputStreamOperator<OrderInfo> orderInfoDS = orderInfoJsonStrDS.map(
            new RichMapFunction<String, OrderInfo>() {
                SimpleDateFormat sdf = null;

                @Override
                public void open(Configuration parameters) throws Exception {
                    sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                }

                @Override
                public OrderInfo map(String jsonStr) throws Exception {
                    OrderInfo orderInfo = JSON.parseObject(jsonStr, OrderInfo.class);
                    orderInfo.setCreate_ts(sdf.parse(orderInfo.getCreate_time()).getTime());
                    return orderInfo;
                }
            }
        );

        //3.2 转换订单明细数据结构
        SingleOutputStreamOperator<OrderDetail> orderDetailDS = orderDetailJsonStrDS.map(
            new RichMapFunction<String, OrderDetail>() {
                SimpleDateFormat sdf = null;

                @Override
                public void open(Configuration parameters) throws Exception {
                    sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                }

                @Override
                public OrderDetail map(String jsonStr) throws Exception {
                    OrderDetail orderDetail = JSON.parseObject(jsonStr, OrderDetail.class);
                    orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time()).getTime());
                    return orderDetail;
                }
            }
        );

        //orderInfoDS.print("orderInfo>>>");
        //orderDetailDS.print("orderDetail>>>");

        //TODO 4. 指定事件时间字段
        //4.1 订单指定事件时间字段
        SingleOutputStreamOperator<OrderInfo> orderInfoWithTsDS = orderInfoDS.assignTimestampsAndWatermarks(
            WatermarkStrategy
                .<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo orderInfo, long recordTimestamp) {
                        return orderInfo.getCreate_ts();
                    }
                })
        );
        //4.2 订单明细指定事件时间字段
        SingleOutputStreamOperator<OrderDetail> orderDetailWithTsDS = orderDetailDS.assignTimestampsAndWatermarks(
            WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(
                    new SerializableTimestampAssigner<OrderDetail>() {
                        @Override
                        public long extractTimestamp(OrderDetail orderDetail, long recordTimestamp) {
                            return orderDetail.getCreate_ts();
                        }
                    }
                )
        );


        //TODO 5.按照订单id进行分组  指定关联的key
        KeyedStream<OrderInfo, Long> orderInfoKeyedDS = orderInfoWithTsDS.keyBy(OrderInfo::getId);
        KeyedStream<OrderDetail, Long> orderDetailKeyedDS = orderDetailWithTsDS.keyBy(OrderDetail::getOrder_id);

        //TODO 6.使用intervalJoin对订单和订单明细进行关联
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoKeyedDS
            .intervalJoin(orderDetailKeyedDS)
            .between(Time.milliseconds(-5), Time.milliseconds(5))
            .process(
                new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                }
            );



        //orderWideDS.print("orderWide>>>>");

        //TODO 7.关联用户维度
        AsyncDataStream.unorderedWait(
            orderWideDS,
            new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                @Override
                public String getKey(OrderWide orderWide) {
                    return orderWide.getUser_id().toString();
                }

                @Override
                public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) throws Exception {
                    //获取用户生日
                    String birthday = dimInfoJsonObj.getString("BIRTHDAY");
                    //定义日期转换工具类
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    //将生日字符串转换为日期对象
                    Date birthdayDate = sdf.parse(birthday);
                    //获取生日日期的毫秒数
                    Long birthdayTs = birthdayDate.getTime();

                    //获取当前时间的毫秒数
                    Long curTs = System.currentTimeMillis();

                    //年龄毫秒数
                    Long ageTs = curTs - birthdayTs;
                    //转换为年龄
                    Long ageLong = ageTs / 1000L / 60L / 60L / 24L / 365L;
                    Integer age = ageLong.intValue();

                    //将维度中的年龄赋值给订单宽表中的属性
                    orderWide.setUser_age(age);

                    //将维度中的性别赋值给订单宽表中的属性
                    orderWide.setUser_gender(dimInfoJsonObj.getString("GENDER"));
                }
            },
            60, TimeUnit.SECONDS);
        env.execute();
    }
}
