package com.yjx.operator.connectors;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName : Flink051012SinkToKafka
 * @Description : Flink051012SinkToKafka
 * @Author : YangJiuZhou
 * @Date: 2023-05-10 20:44
 */
public class Flink051012SinkToKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(2);

//        mock数据
        DataStreamSource<String> source = executionEnvironment.socketTextStream("localhost", 19999);

//       kafka输出配置
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("192.168.63.100:9092,192.168.63.101:9092,192.168.63.102:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("flink_topic")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        source.map(word -> word + ":" + System.currentTimeMillis()).sinkTo(sink);

        executionEnvironment.execute();
    }
}
