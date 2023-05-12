package com.yjx.flink;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName : Hello03WorgdCountUseDataStreamByJava
 * @Description : Hello03WordCountUseDataStreamByJava
 * @Author : YangJiuZhou
 * @Date: 2023-05-09 15:25
 */
public class Hello03WordCountUseDataStreamByJava {
    public static void main(String[] args) throws Exception {
//        获取环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

//        1.Source
        DataStream<String> source = environment.socketTextStream("localhost", 19666);
        SingleOutputStreamOperator<String> flatMap = source.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(word);
                }
            }
        });

//
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = flatMap.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return Tuple2.of(word, 1);
            }
        });

//

        KeyedStream<Tuple2<String, Integer>, String> keyBy = map.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.f0;
            }
        });

//
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyBy.sum(1);

//        3.Sink
        sum.print();

//        运行环境
        environment.execute();
    }
}
