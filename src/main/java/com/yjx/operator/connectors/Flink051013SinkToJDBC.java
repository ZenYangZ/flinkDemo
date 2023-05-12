package com.yjx.operator.connectors;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName : Flink051012SinkToJDBC
 * @Description : Flink051012SinkToJDBC
 * @Author : YangJiuZhou
 * @Date: 2023-05-10 21:29
 */
public class Flink051013SinkToJDBC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);

//    添加操作数据
        List<Tuple3<String, String, Integer>> list = new ArrayList<>();
        list.add(Tuple3.of(String.valueOf(RandomStringUtils.randomAlphabetic(8)), "张三1",13));
        list.add(Tuple3.of(String.valueOf(RandomStringUtils.randomAlphabetic(8)), "张三2",14));
        list.add(Tuple3.of(String.valueOf(RandomStringUtils.randomAlphabetic(8)), "张三3",15));
        list.add(Tuple3.of(String.valueOf(RandomStringUtils.randomAlphabetic(8)), "张三4",16));

//        1.source
        DataStreamSource<Tuple3<String, String, Integer>> tuple3DataStreamSource = environment.fromCollection(list);

//        写出数据
        tuple3DataStreamSource.addSink(
                JdbcSink.sink(
                "insert into personInformation (id, name, age) values (?, ?, ?)",
                (statement, personInformation) -> {
                    statement.setString(1, personInformation.f0);
                    statement.setString(2, personInformation.f1);
                    statement.setInt(3, personInformation.f2);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://192.168.63.100:3306/flink_test?useSSL=false&useUnicode=true&characterEncoding=UTF8&serverTimezone=GMT")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
        ));

        environment.execute();
    }
}
