package com.yjx.operator.common;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName : Flink05108TransformationReduce
 * @Description : Flink05108TransformationReduce
 * @Author : YangJiuZhou
 * @Date: 2023-05-10 19:22
 */
public class Flink05108TransformationReduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        streamExecutionEnvironment.setParallelism(1);

//      source
        DataStreamSource<String> source = streamExecutionEnvironment.fromElements("a", "aa", "b", "bb", "c", "cc");
//      transformation
//        根据word.length() 根据字符的长度进行分组，字符长度相同的放在一组进行比较
        source.keyBy(word -> word.length()).reduce((v1, v2) -> v1 + "," + v2).print();

//      运行环境执行
        streamExecutionEnvironment.execute();

        /* 计算结果，是一个逐步叠加结果的过程
        a
        aa
        a,b
        aa,bb
        a,b,c
        aa,bb,cc
        */

    }
}
