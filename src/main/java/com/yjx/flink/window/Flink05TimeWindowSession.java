package com.yjx.flink.window;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @ClassName : Flink05TimeWindowSession
 * @Description : Flink05TimeWindowSession
 * @Author : YangJiuZhou
 * @Date: 2023-05-13 16:32
 */
public class Flink05TimeWindowSession {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> source = executionEnvironment.socketTextStream("localhost", 19999);

        //TimeWindow--Session
        source.map(word -> Tuple2.of(word.split(":")[0], Integer.parseInt(word.split(":")[1])), Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tuple2 -> tuple2.f0)
//                基于Session会话的TimeWindow窗口
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .reduce((t1, t2) -> {
                    t1.f1 = t1.f1 + t2.f1;
                    return t1;
                })
                .map(tuple2 -> {
                    tuple2.f0 = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy年MM月dd日HH时mm分ss秒SSS毫秒")) + tuple2.f0;
                    return tuple2;
                }, Types.TUPLE(Types.STRING, Types.INT))
                .print("TimeWindow--Session:").setParallelism(1);

        //运行环境
        executionEnvironment.execute();
    }

}
