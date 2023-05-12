package com.yjx.test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.util.Collector;

/**
 * @ClassName : testJava
 * @Description : testJava
 * @Author : YangJiuZhou
 * @Date: 2023-05-10 08:42
 */
public class testJava {
    public static void main(String[] args) {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
//        ExecutionEnvironment没有socketTextStream方法
        DataSource<String> source = environment.readTextFile("data/wordcount.txt");

//        Transformation
        FlatMapOperator<String, String> flatmap = source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(word);
                }
            }
        });


        flatmap.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return s.length();
            }
        });
    }
}
