package com.yjx.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName : Hello11SetTaskParallelism
 * @Description : Hello11SetTaskParallelism
 * @Author : YangJiuZhou
 * @Date: 2023-05-09 15:50
 */
public class Hello11SetTaskParallelism {
    public static void main(String[] args) {
//        获取环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

//        1.Source
        DataStream<String> source = environment.socketTextStream("localhost", 19666);

//        2.Transformation  完善代码(添加parallelism)
        source.flatMap((line, collector) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        collector.collect(word);
                    }
                }, Types.STRING)
                .map(word -> Tuple2.of(word, 1), Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tuple2 -> tuple2.f0)
                .sum(1).setParallelism(3)
                .print().setParallelism(3);

        //打印执行计划
        System.out.println(environment.getExecutionPlan());
    }
}
