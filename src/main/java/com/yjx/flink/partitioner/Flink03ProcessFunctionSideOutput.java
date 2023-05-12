package com.yjx.flink.partitioner;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @ClassName : Flink03ProcessFunctionSideOutput
 * @Description : Flink03ProcessFunctionSideOutput
 * @Author : YangJiuZhou
 * @Date: 2023-05-12 20:57
 */
public class Flink03ProcessFunctionSideOutput {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = executionEnvironment.fromElements("a", "aa", "aaa", "b", "bb", "bbb");

        //进行进行处理--根据长度收集数据并进行计算
        // DataStream<String> s1 = source.filter(w -> w.length() == 1);
        // DataStream<String> s2 = source.filter(w -> w.length() == 2);
        
//        但是如何实现  只读取一次，然后将 等于1 和等于2 的结果分别输出出来呢
        OutputTag<String> outputTag1 = new OutputTag<String>("outputTag1"){};
        OutputTag<String> outputTag2 = new OutputTag<String>("outputTag2"){};

        SingleOutputStreamOperator<String> sideOutput = source.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
//                判断字符串的长度并将其放入到侧输出
                if (value.length() == 1) {
                    ctx.output(outputTag1, value);
                } else if (value.length() == 2) {
                    ctx.output(outputTag2, value);
                } else {
                    out.collect(value.toUpperCase());
                }
            }
        });

//        获取侧输出的数据
        sideOutput.getSideOutput(outputTag1).print("侧输出1:").setParallelism(1);
        sideOutput.getSideOutput(outputTag2).print("侧输出2:").setParallelism(1);

//        查询侧输出的数据
        sideOutput.print();

        executionEnvironment.execute();
    }
}
