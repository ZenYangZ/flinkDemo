package com.yjx.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName : Hello09WordCountTypes
 * @Description : Hello09WordCountTypes
 * @Author : YangJiuZhou
 * @Date: 2023-05-09 15:49
 */
public class Hello09WordCountTypesFunctional {
    public static void main(String[] args) throws Exception {
//        获取环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

//        1.Source
        DataStream<String> source = environment.socketTextStream("localhost", 19666);

//        2.transformation
        source.flatMap((line, collector) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                collector.collect(word);
            }
        }, Types.STRING)
        .map(word -> Tuple2.of(word, 1), Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(tuple2 -> tuple2.f0)
                .sum(1).print();


//        运行环境
        environment.execute();
    }
}
