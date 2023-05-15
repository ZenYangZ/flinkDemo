package com.yjx.flink.window;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @ClassName : Flink12WindowFunctionsByWindow
 * @Description : Flink12WindowFunctionsByWindow
 * @Author : YangJiuZhou
 * @Date: 2023-05-14 12:37
 */
public class Flink12WindowFunctionsByWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> source = executionEnvironment.socketTextStream("localhost", 19999);

        //TimeWindow--Sliding
        source.map(word -> Tuple2.of(word.split(":")[0], Integer.parseInt(word.split(":")[1])), Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tuple2 -> tuple2.f0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(5)))
//                 the Apply operator is used to apply a function to a keyed stream of data
//                全量窗口  new WindowFunction
                .apply(new WindowFunction<Tuple2<String, Integer>, Tuple3<String, Integer, String>, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple3<String, Integer, String>> out) throws Exception {
                        //计算总和
                        int sum = 0;
                        for (Tuple2<String, Integer> tuple2 : input) {
                            sum += tuple2.f1;
                        }
                        out.collect(Tuple3.of(s, sum, window.toString()));
                    }
                })
                .print("TimeWindow--Sliding:").setParallelism(1);

        //运行环境
        executionEnvironment.execute();

//        The processElement method is called for each input element in the stream, and you can use it to process the element and emit zero or more output elements.
//        The onTimer method is called when a timer you previously registered fires, and you can use it to perform some action based on the timer firing.
    }

}
