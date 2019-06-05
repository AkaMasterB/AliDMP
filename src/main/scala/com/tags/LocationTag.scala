package com.tags

import com.common.Tags
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 功能描述:
  * 〈 地域标签 〉
  *
  * @since: 1.0.0
  * @Author:SiXiang
  * @Date: 2019/6/5 19:43
  */
object LocationTag extends Tags {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]

    val provinceName = row.getAs[String]("provincename")
    val cityName = row.getAs[String]("cityname")
    if (StringUtils.isNotBlank(provinceName)) {
      list :+= (provinceName, 1)
    }
    if (StringUtils.isNotBlank(cityName)) {
      list :+= (cityName, 1)
    }
    list
  }
}
