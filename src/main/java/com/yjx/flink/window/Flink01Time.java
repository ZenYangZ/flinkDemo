package com.yjx.flink.window;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @ClassName : Flink01Time
 * @Description : Flink01Time
 * @Author : YangJiuZhou
 * @Date: 2023-05-13 16:03
 */
public class Flink01Time {
    public static void main(String[] args) {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        //1.12以前--给所有的流设置时间语义
        // environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        // environment.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        // environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


//        1.12之后
        DataStreamSource<String> source = executionEnvironment.fromElements("aa", "bb", "cc", "dd");
        source.keyBy(w -> w)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
    }

}
