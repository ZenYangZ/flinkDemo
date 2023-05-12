package com.yjx.operator.common;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @ClassName : Flink051010TansformationUnionConnect
 * @Description : Flink051010TansformationUnionConnect
 * @Author : YangJiuZhou
 * @Date: 2023-05-10 20:23
 */
public class Flink051010TansformationUnionConnect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(2);

//        定义两个source源
        DataStreamSource<String> source1 = executionEnvironment.fromElements("yes", "yes", "no", "yes");
        DataStreamSource<Double> source2 = executionEnvironment.fromElements(33.1, 33.6, 50.2, 33.3);

        ConnectedStreams<String, Double> connect = source1.connect(source2);

        connect.map(new CoMapFunction<String, Double, String>() {
            @Override
            public String map1(String value) throws Exception {
//                这里将"yes" 放在前面可以避免NullPointerException问题，map2方法同理
                if ("yes".equals(value)) {
                    return "[" + value + "] 火警传感器[安全]";
                }
                return "[" + value + "] 火警传感器[危险]";
            }

            @Override
            public String map2(Double value) throws Exception {
                if (50.0 >= value) {
                    return "[" + value + "] 温度传感器[安全]";
                }
                return "[" + value + "] 温度传感器[危险]";
            }
        }).print();

        executionEnvironment.execute();

    }
}
