package com.yjx.flink.window;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @ClassName : Flink04TimeWindow
 * @Description : Flink04TimeWindow
 * @Author : YangJiuZhou
 * @Date: 2023-05-13 16:22
 */
public class Flink04TimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> source = executionEnvironment.socketTextStream("localhost", 19999);

        //TimeWindow--Tumbling
         /*source.map(word -> Tuple2.of(word.split(":")[0], Integer.parseInt(word.split(":")[1])), Types.TUPLE(Types.STRING, Types.INT))
                 .keyBy(tuple2 -> tuple2.f0)
//                 注意这里的算子不是window(不是countWindow)，而且TumblingProcessingTimeWindows作为参数传进来
                 .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                 .reduce((t1, t2) -> {
                     t1.f1 = t1.f1 + t2.f1;
                     return t1;
                 })
//                 使用map算子，将数据的0号位索引元素添加时间
                 .map(tuple2 -> {
                   tuple2.f0 = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")) + tuple2.f0;
                   return tuple2;
                 },Types.TUPLE(Types.STRING,Types.INT))
                 .print("TimeWindow-Tumbling").setParallelism(1);*/


        //TimeWindow--Sliding
        source.map(word -> Tuple2.of(word.split(":")[0], Integer.parseInt(word.split(":")[1])), Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tuple2 -> tuple2.f0)
//                 注意这里的算子不是window(不是countWindow)，而且TumblingProcessingTimeWindows作为参数传进来
//                相较于上面的代码增加了 slide滑动时间
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5),Time.seconds(2)))
                .reduce((t1, t2) -> {
                    t1.f1 = t1.f1 + t2.f1;
                    return t1;
                })
//                 使用map算子，将数据的0号位索引元素添加时间
                .map(tuple2 -> {
                    tuple2.f0 = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")) + tuple2.f0;
                    return tuple2;
                },Types.TUPLE(Types.STRING,Types.INT))
                .print("TimeWindow-Tumbling").setParallelism(1);

         executionEnvironment.execute();
    }

}
