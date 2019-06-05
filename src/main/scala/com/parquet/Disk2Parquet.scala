package com.parquet

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Disk2Parquet {
  def main(args: Array[String]): Unit = {
    // 首先判断目录是否为空
    if(args.length != 2){
      // 退出程序
      println("目录输入不正确，退出程序！")
      sys.exit()
    }
    // 创建一个集合 存储输入输出目录
    val Array(inputPath,outputPath) = args

    // 创建执行入口
    val conf = new SparkConf().setAppName("parquet").setMaster("local[*]")
      // 采用scala的序列化方式
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    sQLContext.setConf("spark.sql.parquet.compression.codec","snappy")
    // 读取本地文件
    val liens = sc.textFile(inputPath)
    // 过滤字段     满足大于等于85个字段  内部在进行解析字符串的时候，遇到,,,,,,这样的无法解析，会理解为一个字符串
    // 那就没法切割了，所以需要把他们的长度放入split里面，进行切分相同的,,,,,
    val rddLog = liens.map(t=>t.split(",",-1)).filter(_.length >= 85).map(Log(_))

    // 创建DF
    val df = sQLContext.createDataFrame(rddLog)
    // df.show()
    df.write.partitionBy("provincename","cityname").parquet(outputPath)

    sc.stop()

  }
}