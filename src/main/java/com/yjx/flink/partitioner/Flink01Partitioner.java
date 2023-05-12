package com.yjx.flink.partitioner;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName : Flink01Partitioner
 * @Description : Flink01Partitioner
 * @Author : YangJiuZhou
 * @Date: 2023-05-12 20:17
 */
public class Flink01Partitioner {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(2);

        DataStreamSource<String> source = executionEnvironment.readTextFile("data/partition.txt");

        SingleOutputStreamOperator<String> upperStream = source.map(new RichMapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
//                根据上下文获取 每个子任务的索引
                return "task[" + (getRuntimeContext().getIndexOfThisSubtask()) + "]value[" + value + "]";
            }
        }).setParallelism(2);

//        输出数据
//        注意：partitioner算子得出的结果 从1开始算分区数， 而上游子任务索引是从0开始计算
//        upperStream.global().print("GlobalPartitioner").setParallelism(4);  //

//        upperStream.rebalance().print("RebalancePartitioner").setParallelism(3);

//        upperStream.rescale().print("RescalePartitioner").setParallelism(4);

        // upperStream.shuffle().print("ShufflePartitioner").setParallelism(4);  //随机

//        upperStream.broadcast().print("BroadcastPartitioner").setParallelism(2);  //将上游算子的数据输出到下游的所有实例当中

//        upperStream.forward().print("ForwardPartitioner").setParallelism(2);  // 0 -> 1  /  1 -> 2

//        upperStream.keyBy(word -> word).print("KeyGroupStreamPartitioner").setParallelism(4);  //keyby 未进行任何操作

//        自定义分区器
        upperStream.partitionCustom(new Partitioner<String>() {
            @Override
            public int partition(String key, int numPartitions) {
//                为什么这里是代表 选取最后一个分区
                return numPartitions - 1;
            }
        }, new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
//                将值传入，然后返回，没有进行任何操作
                return value;
            }
        }).print("CustomPartitioner").setParallelism(4);


//        自定义分区简化版
//        upperStream.partitionCustom((k,n) -> n - 1, v -> v).print("CustomPartitioner").setParallelism(4);




//        运行环境
        executionEnvironment.execute();
    }
}
