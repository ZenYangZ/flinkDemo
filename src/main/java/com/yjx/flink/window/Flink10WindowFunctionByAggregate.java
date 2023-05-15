package com.yjx.flink.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName : Flink10WindowFunctionByAggregate
 * @Description : Flink10WindowFunctionByAggregate
 * @Author : YangJiuZhou
 * @Date: 2023-05-13 17:20
 */
public class Flink10WindowFunctionByAggregate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> source = executionEnvironment.socketTextStream("localhost", 19999);

        //CountWindow--Tumbling--增量计算
        source.map(word -> Tuple2.of(word.split(":")[0],Integer.parseInt(word.split(":")[1])), Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(tuple2 -> tuple2.f0)
                .countWindow(3)
                .aggregate(new AggregateFunction<Tuple2<String, Integer>, Tuple2<Integer,Integer>, Double>() {
                    @Override
                    public Tuple2<Integer, Integer> createAccumulator() {
//                        初始化累加器
                        return Tuple2.of(0, 0);
                    }

                    @Override
                    public Tuple2<Integer, Integer> add(Tuple2<String, Integer> value, Tuple2<Integer, Integer> accumulator) {
//                        累加器的key(f0)是统计 Tuple2的value(f1)
                        accumulator.f0 = accumulator.f0 + value.f1;
//                        累加器的次数加1
                        accumulator.f1 += 1;
                        return accumulator;
                    }

                    @Override
                    public Double getResult(Tuple2<Integer, Integer> accumulator) {
//                        判断除数不能为零
                        if (accumulator.f1 == 0){
                            return 0.0;
                        }
//                        这里乘的1.0是为了将int类型转换成long类型，而且必须在除数上乘1.0，如果在除数上乘 由于 乘法和除法优先级相同，会导致不会
                        return accumulator.f0 * 1.0/accumulator.f1;
                    }

                    @Override
                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                        return null;
                    }
                }).print("CountWindow--Tumbling").setParallelism(1);

        executionEnvironment.execute();
    }

}
