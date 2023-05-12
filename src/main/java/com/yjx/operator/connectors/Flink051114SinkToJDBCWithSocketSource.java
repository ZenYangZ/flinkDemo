package com.yjx.operator.connectors;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;



/**
 * @ClassName : Flink051114SinkToJDBCWithSocketSource
 * @Description : Flink051114SinkToJDBCWithSocketSource
 * @Author : YangJiuZhou
 * @Date: 2023-05-11 09:35
 */
public class Flink051114SinkToJDBCWithSocketSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
//

//    添加操作数据
        DataStreamSource<String> source = environment.socketTextStream("localhost", 19999);
        SingleOutputStreamOperator<Tuple3<Integer, String, Integer>> mapStream = source.map(word -> {
            String[] split = word.split(":");
            return Tuple3.of(Integer.parseInt(split[0]), split[1], Integer.parseInt(split[2]));
        }, Types.TUPLE(Types.INT,Types.STRING,Types.INT));

//        写出数据
        mapStream.addSink(
                JdbcSink.sink(
                        "insert into flink_test_jdbc2 (id, company, age) values (?, ?, ?)",
                        (statement, personInformation) -> {
                            statement.setInt(1, personInformation.f0);
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
