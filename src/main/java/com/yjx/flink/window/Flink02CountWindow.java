package com.yjx.flink.window;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName : Flink02CountWindow
 * @Description : Flink02CountWindow
 * @Author : YangJiuZhou
 * @Date: 2023-05-13 16:06
 */
public class Flink02CountWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = environment.socketTextStream("localhost", 19999);

//        countWindow  --Tumbling
//        将word根据 : 进行切分，然后 将 1号索引元素转成Int类型，然后写上类型推断Types...
/*        source.map(word -> Tuple2.of(word.split(":")[0],Integer.parseInt(word.split(":")[1])), Types.TUPLE(Types.STRING,Types.INT))
//                根据上面切分之后的0号索引元素切分
                .keyBy(tuple2 -> tuple2.f0)
//                使用countWindow算子，达到3个event就关闭窗口，进行计算
                .countWindow(3)
//                设计窗口之后，目的是为了聚合窗口中的数据，之前
//                已经keyBy分过区了，表示key相同，那么将f1就代表value，这里是将value进行聚合
                .reduce((t1,t2) -> {
                    t1.f1 = t1.f1 + t2.f1;
                    return t1;
                }).print("CountWindow--Tumbling:").setParallelism(1);*/

//        countWindow --Sliding
        source.map(word -> Tuple2.of(word.split(":")[0],Integer.parseInt(word.split(":")[1])), Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(tuple2 -> tuple2.f0)
//                注意这里相对于上面代码多加了一个slide参数，表示多少个元素更新一次窗口，
//                这里slide < size， 表示每次更新窗口会有一个数据重复
                .countWindow(3,2)
                .reduce((t1,t2) -> {
                    t1.f1 = t1.f1 + t2.f1;
                    return t1;
                }).print("CountWindow--Tumbling:").setParallelism(1);

//        运行环境
        environment.execute();

    }

}
