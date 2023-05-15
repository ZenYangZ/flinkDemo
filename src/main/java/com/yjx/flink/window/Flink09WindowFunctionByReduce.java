package com.yjx.flink.window;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName : Flink09WindowFunctionByReduce
 * @Description : Flink09WindowFunctionByReduce
 * @Author : YangJiuZhou
 * @Date: 2023-05-13 17:11
 */
public class Flink09WindowFunctionByReduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> source = executionEnvironment.socketTextStream("localhost", 19999);

//        1.增量计算的条件为：基于 WindowedStream 调用.reduce()方法，然后传入 ReduceFunction 作为参数，
//        就可以指定以归约两个元素的方式去对窗口中数据进行聚合了

//        2.不足：ReduceFunction可以解决大多数的归约聚合问题，
//        但这个接口有一个限制就是聚合状态的类型、输出结果的类型必须和输入类型一样
//        CountWindow增量计算
        source.map(word -> Tuple2.of(word.split(":")[0], Integer.parseInt(word.split(":")[1])), Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tuple2 -> tuple2.f0)
                .countWindow(3)
                .reduce((t1, t2) -> {
                    System.out.println("窗口增量计算函数-来一条算一条.main[" + t1 + "][" + t2 + "]");
                    t1.f0 = t1.f0 + "_" + t2.f0;
                    t1.f1 = t1.f1 + t2.f1;
                    return t1;
                }).print("CountWindow--Tumbling:").setParallelism(1);

        //运行环境
        executionEnvironment.execute();
    }

}
