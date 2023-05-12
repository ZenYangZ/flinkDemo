package com.yjx.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName : Hello06WordCountUseDataStream
 * @Description : Hello06WordCountUseDataStream
 * @Author : YangJiuZhou
 * @Date: 2023-05-09 15:48
 */
public class Hello06WordCountUseDataStreamErrorInstance {
//    这里是错误的案例，无法运行，Java无法推断泛型中还包含泛型的类型，Java的缺点
    public static void main(String[] args) throws Exception {
//        获取环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
//        1.source
        DataStreamSource<String> source = environment.socketTextStream("localhost", 19666);

//        2.Transformation
        SingleOutputStreamOperator<Tuple2<Object,Integer>> sum = source.flatMap((line, collector) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                collector.collect(word);
            }
        }).map(word -> Tuple2.of(word, 1))
            .keyBy(tuple2 -> tuple2.f0)
                .sum(1);

//       3.sink
        sum.print();

//       4.运行环境
        environment.execute();
    }

}
