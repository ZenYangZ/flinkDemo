package com.yjx.operator.connectors;

import com.yjx.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName : Flink0510SourceFromKafka
 * @Description : Flink0510SourceFromKafka
 * @Author : YangJiuZhou
 * @Date: 2023-05-10 15:22
 */
public class Flink05103SourceFromKafka {
    public static void main(String[] args) throws Exception {
        //创建一个线程持续向Kafka添加数据
        new Thread(() -> {
            for (int i = 0; i < 100; i++) {
                KafkaUtil.sendMsg("yjxxt", "Hello Flink Kakfa" + i + "," + System.currentTimeMillis());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        //执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.Source
        KafkaSource<String> sourceSetting = KafkaSource.<String>builder()
                .setBootstrapServers("192.168.63.100:9092,192.168.63.101:9092,192.168.63.102:9092")
                .setTopics("yjxxt")
                .setGroupId("msbCS")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStreamSource<String> source = environment.fromSource(sourceSetting, WatermarkStrategy.noWatermarks(), "Kafka Source");
        //2.Transformation+3.Sink
        source.map(word -> word.toUpperCase()).print();
        //运行环境
        environment.execute();
    }

/*
* 报错信息
*
* 原因
* Some post says its because topic creation is disabled, but it's not in my case. The storage topic is still created.
* 经过一段时间没有使用该topic或者其他情况导致 该topic失效，创建一个新的topic或者使用没有失效的topic即可*/
//    没有新建topic使用导致报错
    /* 报错信息
10:33:56.706 [DistributedHerder] WARN  org.apache.kafka.clients.admin.AdminClientConfig - The configuration 'config.storage.topic' was supplied but isn't a known config.
10:33:56.707 [DistributedHerder] WARN  org.apache.kafka.clients.admin.AdminClientConfig - The configuration 'group.id' was supplied but isn't a known config.
10:33:56.708 [DistributedHerder] WARN  org.apache.kafka.clients.admin.AdminClientConfig - The configuration 'status.storage.topic' was supplied but isn't a known config.
10:33:56.709 [DistributedHerder] WARN  org.apache.kafka.clients.admin.AdminClientConfig - The configuration 'internal.key.converter.schemas.enable' was supplied but isn't a known config.
10:33:56.710 [DistributedHerder] WARN  org.apache.kafka.clients.admin.AdminClientConfig - The configuration 'config.storage.replication.factor' was supplied but isn't a known config.
10:33:56.710 [DistributedHerder] WARN  org.apache.kafka.clients.admin.AdminClientConfig - The configuration 'offset.flush.interval.ms' was supplied but isn't a known config.
10:33:56.711 [DistributedHerder] WARN  org.apache.kafka.clients.admin.AdminClientConfig - The configuration 'key.converter.schemas.enable' was supplied but isn't a known config.
10:33:56.712 [DistributedHerder] WARN  org.apache.kafka.clients.admin.AdminClientConfig - The configuration 'internal.key.converter' was supplied but isn't a known config.
10:33:56.712 [DistributedHerder] WARN  org.apache.kafka.clients.admin.AdminClientConfig - The configuration 'internal.value.converter.schemas.enable' was supplied but isn't a known config.
10:33:56.713 [DistributedHerder] WARN  org.apache.kafka.clients.admin.AdminClientConfig - The configuration 'status.storage.replication.factor' was supplied but isn't a known config.
10:33:56.713 [DistributedHerder] WARN  org.apache.kafka.clients.admin.AdminClientConfig - The configuration 'value.converter.schemas.enable' was supplied but isn't a known config.
10:33:56.714 [DistributedHerder] WARN  org.apache.kafka.clients.admin.AdminClientConfig - The configuration 'internal.value.converter' was supplied but isn't a known config.
10:33:56.714 [DistributedHerder] WARN  org.apache.kafka.clients.admin.AdminClientConfig - The configuration 'offset.storage.replication.factor' was supplied but isn't a known config.
10:33:56.715 [DistributedHerder] WARN  org.apache.kafka.clients.admin.AdminClientConfig - The configuration 'offset.storage.topic' was supplied but isn't a known config.
10:33:56.715 [DistributedHerder] WARN  org.apache.kafka.clients.admin.AdminClientConfig - The configuration 'value.converter' was supplied but isn't a known config.
10:33:56.716 [DistributedHerder] WARN  org.apache.kafka.clients.admin.AdminClientConfig - The configuration 'key.converter' was supplied but isn't a known config.**/
}
