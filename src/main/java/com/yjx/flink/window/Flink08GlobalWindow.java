package com.yjx.flink.window;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;

/**
 * @ClassName : Flink08GlobalWindow
 * @Description : Flink08GlobalWindow
 * @Author : YangJiuZhou
 * @Date: 2023-05-13 16:56
 */
public class Flink08GlobalWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> source = executionEnvironment.socketTextStream("localhost", 19999);

        source.map(word -> Tuple2.of(word.split(":")[0],Integer.parseInt(word.split(":")[1])), Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(tuple2 -> tuple2.f0)
//                使用window算子 并设置GlobalWindows.create()参数
                .window(GlobalWindows.create())
//                配置window计算触发条件

//                1.PurgingTrigger.of():  Creates a new purging trigger from the given Trigger.
//                  Params: nestedTrigger – The trigger that is wrapped by this purging trigger
//                2.向PurgingTrigger.of() 中传入一个Trigger，返回一个嵌套的Trigger

//                3.When the nested trigger fires, this will return a FIRE_AND_PURGE TriggerResult.

                .trigger(PurgingTrigger.of(CountTrigger.of(5)))
                .reduce((t1,t2) -> {
                    t1.f1 = t1.f1 + t2.f1;
                    return t1;
                }).print("GlobalWindow:").setParallelism(1);

//        windowAll使用 GlobalWindows.create()参数
        source.map(word -> Tuple2.of(word.split(":")[0], Integer.parseInt(word.split(":")[1])), Types.TUPLE(Types.STRING, Types.INT))
                .windowAll(GlobalWindows.create())
                .trigger(PurgingTrigger.of(CountTrigger.of(5)))
                .reduce((t1, t2) -> {
                    t1.f1 = t1.f1 + t2.f1;
                    return t1;
                }).print("GlobalWindow:").setParallelism(1);

        //运行环境
        executionEnvironment.execute();

    }

}
