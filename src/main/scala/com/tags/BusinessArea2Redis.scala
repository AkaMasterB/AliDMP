package com.tags

import ch.hsr.geohash.GeoHash
import com.utils.{BaiduLBSHandler, JedisConnectionPool}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 功能描述:
  * 〈 将解析后的商圈存入redis ,借用 GeoHash 算法编码后将区域作为 key〉
  *
  * @since: 1.0.0
  * @Author:SiXiang
  * @Date: 2019/6/6 15:48
  */
object BusinessArea2Redis {

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      // 退出程序
      println("目录输入不正确，退出程序！")
      sys.exit()
    }
    // 创建一个集合 存储输入输出目录
    val Array(inputPath) = args

    val conf = new SparkConf().setAppName("parquet").setMaster("local[*]")
      // 采用scala的序列化方式
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    sQLContext.setConf("spark.sql.parquet.compression.codec", "snappy")

    sQLContext.read.parquet(inputPath)
      .select("lat", "long")
      .filter( //取国内范围
        """
          |cast(long as double) >= 73 and cast(long as double) <= 136 and
          |cast(lat as double) >= 3 and cast(lat as double) <= 54
        """.stripMargin)
      .distinct()
      .foreachPartition(partition => {
        // 获取 redis 客户端 jedis 对象
        val jedis = JedisConnectionPool.getConnection()
        partition.foreach(t => {
          val lng = t.getAs[String]("long")
          val lat = t.getAs[String]("lat")
          val businessArea = BaiduLBSHandler.parseBusinessTagBy(lng, lat)
          val geoHash = GeoHash.geoHashStringWithCharacterPrecision(lat.toDouble, lng.toDouble, 8)
          jedis.set(geoHash, businessArea)
        })
        // 关闭连接
        jedis.close()
      })


  }
}
