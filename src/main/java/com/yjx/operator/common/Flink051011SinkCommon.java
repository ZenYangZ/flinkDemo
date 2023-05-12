package com.yjx.operator.common;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.charset.StandardCharsets;

/**
 * @ClassName : Flink051011SinkCommon
 * @Description : Flink051011SinkCommon
 * @Author : YangJiuZhou
 * @Date: 2023-05-10 20:33
 */
public class Flink051011SinkCommon {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(2);

//        1.source
        DataStreamSource<Integer> integerDataStreamSource = executionEnvironment.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

//        打印到控制台
//        integerDataStreamSource.print();

//        写出到文件
        integerDataStreamSource.map(num -> {
            return Tuple2.of(num,0);
        },Types.TUPLE(Types.INT,Types.INT))
        .writeAsCsv("data/csc_" + System.currentTimeMillis());

//        写出到socket
//        integerDataStreamSource.writeToSocket("192.168.63.100", 20523, new SerializationSchema<Integer>() {
//            @Override
//            public byte[] serialize(Integer element) {
//                return element.toString().getBytes(StandardCharsets.UTF_8);
//            }
//        });

//        运行环境执行
        executionEnvironment.execute();
    }
}
