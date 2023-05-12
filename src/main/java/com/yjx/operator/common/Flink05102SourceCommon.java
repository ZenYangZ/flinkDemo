package com.yjx.operator.common;


import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName : Flink0510SourceCommon
 * @Description : Flink0510SourceCommon
 * @Author : YangJiuZhou
 * @Date: 2023-05-10 15:00
 */
public class Flink05102SourceCommon {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

//        1.根据文本文件获取数据源
//        DataStreamSource<String> source = streamExecutionEnvironment.readTextFile("data/wordcount.txt");

//        2.根据文件获取数据源，这里尝试使用csv文件作为数据源source
//        streamExecutionEnvironment.readFile(new CsvInputFormat<Object>("") {
//            @Override
//            protected Object fillRecord(Object reuse, Object[] parsedValues) {
//                return null;
//            }
//        });

//       3.基于socket
//        DataStreamSource<String> source = streamExecutionEnvironment.socketTextStream("localhost", 19999);

//       4.基于Collection   --用于测试或者代码的开发
//        List<String> list = List.of("aa", "bb", "cc", "dd");
//        DataStreamSource<String> source = streamExecutionEnvironment.fromCollection(list);

//       5.从给定的对象序列中创建数据流
        DataStreamSource<String> source = streamExecutionEnvironment.fromElements("aa", "b", "c");

//       6.从迭代器中并行创建数据流

//       7.基于给定间隔内的数字序列并行生成数据流

//      转换和sink
        source.map(word -> word.toLowerCase()).print();

//        运行环境
        streamExecutionEnvironment.execute();
    }
}
