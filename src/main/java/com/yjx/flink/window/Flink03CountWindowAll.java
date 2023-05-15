package com.yjx.flink.window;



import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName : Flink03CountWindowAll
 * @Description : Flink03CountWindowAll
 * @Author : YangJiuZhou
 * @Date: 2023-05-13 16:18
 */
public class Flink03CountWindowAll {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> source = executionEnvironment.socketTextStream("localhost", 19999);

        //CountWindowAll--Tumbling
        // source.map(word -> Tuple2.of(word.split(":")[0], Integer.parseInt(word.split(":")[1])), Types.TUPLE(Types.STRING, Types.INT))
        //         .countWindowAll(3)
        //         .reduce((t1, t2) -> {
        //             t1.f0=  t1.f0+"_"+t2.f0;
        //             t1.f1 = t1.f1 + t2.f1;
        //             return t1;
        //         }).print("CountWindowAll--Tumbling:").setParallelism(1);

        //CountWindow--Sliding
        source.map(word -> Tuple2.of(word.split(":")[0], Integer.parseInt(word.split(":")[1])), Types.TUPLE(Types.STRING, Types.INT))
                .countWindowAll(3, 2)
                .reduce((t1, t2) -> {
                    t1.f0 = t1.f0 + "_" + t2.f0;
                    t1.f1 = t1.f1 + t2.f1;
                    return t1;
                }).print("CountWindowAll--Sliding:").setParallelism(1);
        //运行环境
        executionEnvironment.execute();
    }

}
