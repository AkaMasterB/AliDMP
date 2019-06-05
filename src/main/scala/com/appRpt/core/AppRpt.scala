package com.appRpt.core

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 功能描述:
  * 〈 APP报表，广播数据字典进行匹配 〉
  *
  * @since: 1.0.0
  * @Author:SiXiang
  * @Date: 2019/6/6 0:15
  */
object AppRpt {
  def main(args: Array[String]): Unit = {
    // 首先判断目录是否为空
    if (args.length != 3) {
      // 退出程序
      println("目录输入不正确，退出程序！")
      sys.exit()
    }
    // 创建一个集合 存储输入输出目录
    val Array(inputPath, outputPath, dictPath) = args

    val conf = new SparkConf().setMaster("local[*]").setAppName("LocationKpi")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    sQLContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    //读取字典文件
    val dictMap = sc.textFile(dictPath)
      .map(_.split("\t", -1))
      .filter(_.length >= 5)
      .map(arr => (arr(4), arr(1)))
      .collect()
      .toMap
    val globalDict = sc.broadcast(dictMap)

    sQLContext.read.parquet(inputPath)
      .map(row => {
        val map = globalDict
        var appName = row.getAs[String]("appname")
        if (!StringUtils.isNotBlank(appName)) {
          appName = map.value.getOrElse(row.getAs[String]("appid"), "unknow")
        }
        val requestMode = row.getAs[Int]("requestmode")
        val processNode = row.getAs[Int]("processnode")
        //  统计其余的指标
        val isEffective = row.getAs[Int]("iseffective")
        val isBilling = row.getAs[Int]("isbilling")
        val isBid = row.getAs[Int]("isbid")
        val isWin = row.getAs[Int]("iswin")
        val adOrderId = row.getAs[Int]("adorderid")
        val winPrice = row.getAs[Double]("winprice")
        val adPayment = row.getAs[Double]("adpayment")
        val reqList = RptUtil.request(requestMode, processNode)
        val bidList = RptUtil.bidding(isEffective, isBilling, isBid, isWin, adOrderId)
        val clickList = RptUtil.showsAndClicks(requestMode, isEffective)
        val dspList = RptUtil.dsp(isEffective, isBilling, isWin, winPrice, adPayment)
        (appName, reqList ++ bidList ++ clickList ++ dspList)
      })
      .reduceByKey((list1, list2) => {
        list1.zip(list2)
          .map(t => t._1 + t._2)
      })
      .map(t => {
        t._1 + "," + t._2.mkString(",")
      })
      .coalesce(1)
      .saveAsTextFile(outputPath)
  }
}
