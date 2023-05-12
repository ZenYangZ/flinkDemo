package com.yjx.operator.common;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @ClassName : Flink05109TransformationIterate
 * @Description : Flink05109TransformationIterate
 * @Author : YangJiuZhou
 * @Date: 2023-05-10 19:42
 */
public class Flink05109TransformationIterate {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

//        操作数据
        DataStreamSource<String> source = executionEnvironment.fromElements("草莓,61", "苹果,101", "桃子,202");
//        迭代每个元素的数量 减去10，当小于10的时候返回结果
//
        DataStream<Tuple3<String, Integer, Integer>> mapStream = source.map(word -> {
            String[] split = word.split(",");
            return Tuple3.of(split[0], Integer.parseInt(split[1]), 0);
        }, Types.TUPLE(Types.STRING,Types.INT,Types.INT));

//        开始将数据进行迭代
        IterativeStream<Tuple3<String, Integer, Integer>> iterativeStream = mapStream.iterate();

//      定义循环体--假设水果每天销售10斤
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> iterativeBody = iterativeStream.map(tuple2 -> {
            tuple2.f1 -= 10;
            tuple2.f2++;
            return tuple2;
        },Types.TUPLE(Types.STRING,Types.INT,Types.INT));

//        定义判断条件
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> iterativeFileter = iterativeBody.filter(tuple3 -> tuple3.f1 > 10);

//      开始迭代
        iterativeStream.closeWith(iterativeFileter);

//        找不出满足条件的数据
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> output = iterativeBody.filter(tuple3 -> tuple3.f1 <= 10);
//      SingleOutputStreamOperator的print方法会将结果输出在print传入的字符串之后
        output.print("不满足条件的数据:");

//        环境执行
        executionEnvironment.execute();
    }
}
