package com.tags

import ch.hsr.geohash.GeoHash
import com.common.Tags
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

/**
  * 功能描述:
  * 〈 商圈标签 〉
  *
  * @since: 1.0.0
  * @Author:SiXiang
  * @Date: 2019/6/6 16:29
  */
object BusinessAreaTag extends Tags{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()

    val row = args(0).asInstanceOf[Row]
    val jedis = args(1).asInstanceOf[Jedis]

    val lng = row.getAs[String]("long")
    val lat = row.getAs[String]("lat")
    val geoHash = GeoHash.geoHashStringWithCharacterPrecision(lat.toDouble, lng.toDouble, 8)

    val businessArea = jedis.get(geoHash)
    if(StringUtils.isNotBlank(lat) && StringUtils.isNotBlank(lng)){
      list :+= (businessArea,1)
    }
    list
  }
}
