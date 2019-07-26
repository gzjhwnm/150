package com.baizhi

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object FlinkWordCount {
  def main(args: Array[String]): Unit = {
    var env = ExecutionEnvironment.getExecutionEnvironment
      env.readTextFile("hdfs:///CentOSD:9000/demo/words")
      .flatMap(_.split("\\s+"))
      .map((_,1))
      .groupBy(0)
      .sum(1)
      .print()
  }
}
