package com.yjx.flink.partitioner;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName : Flink02ProcessFunction
 * @Description : Flink02ProcessFunction
 * @Author : YangJiuZhou
 * @Date: 2023-05-12 20:51
 */
public class Flink02ProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = executionEnvironment.readTextFile("data/wordcount.txt");

//        进行处理
//        1.使用processFunction实现map操作
        source.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                String[] splits = value.split(" ");
                for (String split : splits) {
                    out.collect(split);
                }
            }
//            2.使用process实现
        }).process(new ProcessFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void processElement(String value, ProcessFunction<String, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                out.collect(Tuple2.of(value,1));
            }
        }).keyBy(tuple2 -> tuple2.f0).process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String,Integer>>() {
            @Override
            public void processElement(Tuple2<String, Integer> value, KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

            }
        }).print();

//        运行环境执行
        executionEnvironment.execute();
    }
}
