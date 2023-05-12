package com.yjx.flink

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

import scala.reflect.internal.util.NoPosition.source

object Hello04WordCountUseDataStreamByScala {
  def main(args: Array[String]): Unit = {

    //    获取执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //    1.source
    val source: DataStream[String] = environment.socketTextStream("localhost", 19666)

    //    2.Transformation
    val flatMap: DataStream[String] = source.flatMap(_.split(" "))
    val map: DataStream[(String, Int)] = flatMap.map((_, 1))
    val keyby: KeyedStream[(String, Int), String] = map.keyBy(_._1)
    val sum: DataStream[(String, Int)] = keyby.sum(1)

    //    3.Sink
    sum.print()

    //    运行环境
    environment.execute()
  }

}
