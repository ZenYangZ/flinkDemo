package com.yjx.flink.watermark;


import com.yjx.util.KafkaUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Locale;

/**
 * @ClassName : Flink13WaterMarkInOrder
 * @Description : Flink13WaterMarkInOrder
 * @Author : YangJiuZhou
 * @Date: 2023-05-14 13:01
 */
public class Flink13WaterMarkInOrder {
    public static void main(String[] args) throws Exception {

//        产生kafka有序数据  --模拟弹幕[用户名:消息:时间戳]
        new Thread(() -> {
//            randomAlphabetic:Creates a random string whose length is the number of characters specified.
            String uname = RandomStringUtils.randomAlphabetic(8).toLowerCase(Locale.ROOT);
            for (int i = 0; i <200; i++) {
                KafkaUtil.sendMsg("msb0514",uname + ":" + i + ":" + System.currentTimeMillis());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

//        读取数据源
        DataStreamSource<String> source = executionEnvironment.fromSource(KafkaUtil.getKafkaSource("flink_test", "msb0514"), WatermarkStrategy.noWatermarks(), "Kafka Source");

//        转换数据
        source.map(line -> {
            return Tuple3.of(line.split(":")[0],line.split(":")[1],Long.parseLong(line.split(":")[2]));
        }, Types.TUPLE(Types.STRING,Types.STRING,Types.LONG))
//                使用时间戳和水位线的operator
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String,String,Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                return element.f2;
                            }
                        }))
                .keyBy(tuple3 -> tuple3.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new WindowFunction<Tuple3<String, String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<Tuple3<String, String, Long>> input, Collector<String> out) throws Exception {
                        StringBuffer stringBuffer = new StringBuffer();
                        stringBuffer.append("[key]");
                        for (Tuple3<String, String, Long> tuple3: input){
                            stringBuffer.append("[" + tuple3.f1 + "_" + tuple3.f2 + "]");
                        }
                        stringBuffer.append("[" + window + "]");
//                  返回结果
                        out.collect(stringBuffer.toString());
                    }
                }).print();

//        运行环境
        executionEnvironment.execute();
    }

}
