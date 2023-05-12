package com.yjx.operator.common;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName : Flink05107TransformationAgg
 * @Description : Flink05107TransformationAgg
 * @Author : YangJiuZhou
 * @Date: 2023-05-10 19:10
 */
public class Flink05107TransformationAgg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

//        创建一个存储二元组的列表
        List<Tuple2<String,Integer>> list = new ArrayList<>();

//        添加数据
        list.add(new Tuple2<>("math2", 200));
        list.add(new Tuple2<>("chinese2", 20));
        list.add(new Tuple2<>("math1", 100));
        list.add(new Tuple2<>("chinese1", 10));
        list.add(new Tuple2<>("math4", 400));
        list.add(new Tuple2<>("chinese4", 40));
        list.add(new Tuple2<>("math3", 300));
        list.add(new Tuple2<>("chinese3", 30));

        DataStreamSource<Tuple2<String, Integer>> aggregationSource = environment.fromCollection(list);
//        通过使用keyBy方法，将source转换成keyedStream，滚动聚合算子由keyedStream调用，并生成一个聚合后的DataStream
        KeyedStream<Tuple2<String, Integer>, Integer> keyedStream = aggregationSource.keyBy(new KeySelector<Tuple2<String, Integer>, Integer>() {
            @Override
            public Integer getKey(Tuple2<String, Integer> tuple2) throws Exception {
//
                return tuple2.f0.length();
            }
        });

        /*不同的聚合方式
        * max/min 表示 在输入流上对指定的字段求最大值/最小值
        * max/min 表示 在输入流针对指定字段取最大值/最小值，并且返回最大值所在的那条数据*/

        // keyedStream.sum(1).print("sum-");
        // keyedStream.max(1).print("max-");
        keyedStream.maxBy(1).print("maxBy-");
        // keyedStream.min(1).print("min-");
        // keyedStream.minBy(1).print("minBy-");

//        运行环境
        environment.execute();
    }
}
