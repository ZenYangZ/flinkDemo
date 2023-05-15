package com.yjx.flink.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName : Flink11WindowFunctionByAggregatePlus
 * @Description : Flink11WindowFunctionByAggregatePlus
 * @Author : YangJiuZhou
 * @Date: 2023-05-13 17:46
 */
public class Flink11WindowFunctionByAggregatePlus {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> source = executionEnvironment.socketTextStream("localhost", 19999);

        //CountWindow--Tumbling--增量计算
//        将二元组变成三元组，用来存储

        source.map(word -> Tuple2.of(word.split(":")[0], Integer.parseInt(word.split(":")[1])), Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tuple2 -> tuple2.f0)
                .countWindow(3)
//                                                    在聚合的时候设置三元组   String代表key的类型， Integer代表总和，Integer代表出现次数
                .aggregate(new AggregateFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, Tuple2<String, Double>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> createAccumulator() {
                        return Tuple3.of(null, 0, 0);
                    }

                    @Override
//
                    public Tuple3<String, Integer, Integer> add(Tuple2<String, Integer> in, Tuple3<String, Integer, Integer> acc) {
//                        这里f0是消息的key是什么
                        acc.f0 = in.f0;
//                        这里f1是消息的值
                        acc.f1 += in.f1;
//                        这里f2是 消息出现的次数
                        acc.f2++;
                        return acc;
                    }

                    @Override
                    public Tuple2<String, Double> getResult(Tuple3<String, Integer, Integer> acc) {
                        //判断除数是否为0
                        if (acc.f2 == 0) {
                            return Tuple2.of(acc.f0, 0.0);
                        }
                        return Tuple2.of(acc.f0, acc.f1 * 1.0 / acc.f2);
                    }

                    @Override
                    public Tuple3<String, Integer, Integer> merge(Tuple3<String, Integer, Integer> stringIntegerIntegerTuple3, Tuple3<String, Integer, Integer> acc1) {
                        return null;
                    }
                })
                .print("CountWindow--Tumbling:").setParallelism(1);

        //运行环境
        executionEnvironment.execute();
    }

}
