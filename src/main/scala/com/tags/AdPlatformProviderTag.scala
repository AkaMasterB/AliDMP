package com.tags

import com.common.Tags
import org.apache.spark.sql.Row

/**
  * 功能描述:
  * 〈 渠道标签 〉
  *
  * @since: 1.0.0
  * @Author:SiXiang
  * @Date: 2019/6/5 19:08
  */
object AdPlatformProviderTag extends Tags {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    val row = args(0).asInstanceOf[Row]

    val adPlatformProviderId = row.getAs[Int]("adplatformproviderid")

    if (adPlatformProviderId != null) {
      list :+= ("CN" + adPlatformProviderId, 1)
    }
    list
  }
}
