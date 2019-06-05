package com.appRpt.sql

import com.google.gson.Gson
import com.utils.MySQLJdbcConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object CountInLocation {
  def main(args: Array[String]): Unit = {
    // 首先判断目录是否为空
        if(args.length != 2){
    // 退出程序
          println("目录输入不正确，退出程序！")
          sys.exit()
        }
    // 创建一个集合 存储输入输出目录
        val Array(inputPath,outputPath) = args

    val conf = new SparkConf().setMaster("local[*]").setAppName("LocationKpi")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    sQLContext.setConf("spark.sql.parquet.compression.codec", "snappy")

    sQLContext.read.parquet(args(0))
      .registerTempTable("log")

    val rdd = sQLContext.sql(
      """
         select provincename,cityname,count(*) as ct from log group by provincename,cityname
      """.stripMargin)
      .write.json(outputPath)

  }

}

