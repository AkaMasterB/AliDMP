package com.appRpt.sql

import com.utils.MySQLJdbcConfig
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object NetworkRpt {
  def main(args: Array[String]): Unit = {
    // 首先判断目录是否为空
    //    if(args.length != 2){
    // 退出程序
    //      println("目录输入不正确，退出程序！")
    //      sys.exit()
    //    }
    // 创建一个集合 存储输入输出目录
    //    val Array(inputPath,outputPath) = args

    val conf = new SparkConf().setMaster("local[*]").setAppName("LocationKpi")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    sQLContext.setConf("spark.sql.parquet.compression.codec", "snappy")

    sQLContext.read.parquet(args(0))
      .registerTempTable("log")

    val jdbcConfig = new MySQLJdbcConfig
    jdbcConfig.init()

    sQLContext.sql(
      """
        |select
        |case when devicetype = 1 then '手机' when devicetype = 2 then '平板' else '其他' end devicetype,
        |sum(case when requestmode = 1 and processnode >= 1 then 1 else 0 end) primeval_req,
        |sum(case when requestmode = 1 and processnode >= 2 then 1 else 0 end) valid_req,
        |sum(case when requestmode = 1 and processnode = 3 then 1 else 0 end) ad_req,
        |sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 else 0 end) in_bid,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 else 0 end) success_bid,
        |sum(case when requestmode = 2 and iseffective = 1 then 1 else 0 end) shows,
        |sum(case when requestmode = 3 and iseffective = 1 then 1 else 0 end) clicks,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then winprice/1000 else 0 end) dsp_consume,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment/1000 else 0 end) dsp_cost
        |from log
        |group by devicetype
      """.stripMargin)
      .write.mode("overwrite")
      .jdbc(jdbcConfig.getUrl,"rpt_network",jdbcConfig.getConnectionProperties)
//      .show()
  }
}
