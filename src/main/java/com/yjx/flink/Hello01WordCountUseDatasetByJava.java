package com.yjx.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @ClassName : Hello01WordCountUseDatasetByJava
 * @Description : Hello01WordCountUseDatasetByJava
 * @Author : YangJiuZhou
 * @Date: 2023-05-09 15:06
 */
public class Hello01WordCountUseDatasetByJava {
    public static void main(String[] args) throws Exception {
//        1.获取运行的环境
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
//        2.Source ----------------------------------------------------------------
//        使用readTextFile读取文件作为数据源
        DataSource<String> dataSource = environment.readTextFile("data/wordcount.txt");

//        3.Transformation转换操作
        FlatMapOperator<String, String> flatMapOperator = dataSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                String[] words = line.split(" ");
//                将字符数组遍历
                for (String word : words) {
//                    ??
                    collector.collect(word);
                }

            }
        });
//      将单词通过 空格 来进行切分之后，再通过MapFunction 和 Tuple2(二元组)  来统计每个单词的数量
        MapOperator<String, Tuple2<String, Integer>> mapOperator = flatMapOperator.map(new MapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return Tuple2.of(word, 1);
            }
        });

        AggregateOperator<Tuple2<String, Integer>> sum = mapOperator.groupBy(0).sum(1);

//        4.sink------------------------
        sum.print();

    }

}
