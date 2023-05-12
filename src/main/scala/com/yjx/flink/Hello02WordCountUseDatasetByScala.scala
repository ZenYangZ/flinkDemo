package com.yjx.flink

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment, createTypeInformation}

object Hello02WordCountUseDatasetByScala {
  def main(args: Array[String]): Unit = {
    //    获取执行环境
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //    1.Source
    val source: DataSet[String] = environment.readTextFile("data/wordcount.txt")


    //    2.Transformation
    val flatMapOperator: DataSet[String] = source.flatMap(_.split(" "))
    val mapOperator: DataSet[(String, Int)] = flatMapOperator.map((_, 1))
    val sum: AggregateDataSet[(String, Int)] = mapOperator.groupBy(0).sum(1)

    //    3.Sink
    sum.print()
  }
}
