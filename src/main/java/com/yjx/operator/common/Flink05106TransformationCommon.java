package com.yjx.operator.common;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @ClassName : Flink0510TransformationCommon
 * @Description : Flink0510TransformationCommon
 * @Author : YangJiuZhou
 * @Date: 2023-05-10 17:26
 */
public class Flink05106TransformationCommon {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

//        1.source
        DataStreamSource<String> source = executionEnvironment.fromElements("Hello Hadoop", "Hello Hive", "Hello HBase Phoenix", "Hello   ClickHouse");

//        2.transformation
        source.flatMap((line, collector) -> {
                Arrays.stream(line.split(" ")).forEach(collector::collect);
        }, Types.STRING)
                .filter(word -> word != null && word.length() > 0)
                .map(word -> Tuple2.of(word,(int) (Math.random() * 10)), Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(tuple2 -> tuple2.f0)
                .sum(1)
                .print();

//        运行环境
        executionEnvironment.execute();
    }
}
