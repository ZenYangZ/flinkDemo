package com.yjx.flink

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object Hello05WordCountByDataStreamOptimization {
  def main(args: Array[String]): Unit = {
    //获取执行环境
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    //1.Source
    val source = environment.socketTextStream("localhost", 19666)
    //    2.Transformation
    val sum: DataStream[(String, Int)] = source.flatMap(_.split(" ")).map((_, 1)).keyBy(_._1).sum(1)
    //    3.sink
    sum.print()

    //    运行环境
    environment.execute();
  }
}
