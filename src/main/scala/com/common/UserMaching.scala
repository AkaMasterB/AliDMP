package com.common

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 功能描述:
  * 〈 对15个用户标识字段进行匹配，后续使用 spark graphx 来统一分类 〉
  *
  * @since: 1.0.0
  * @Author:SiXiang
  * @Date: 2019/6/6 0:13
  */
object UserMaching {
  def getAnyUserId(row: Row): String = {
    row match {
      case v if StringUtils.isNoneBlank(v.getAs[String]("imei")) => "IM:" + v.getAs[String]("imei")
      case v if StringUtils.isNoneBlank(v.getAs[String]("mac")) => "MC:" + v.getAs[String]("mac")
      case v if StringUtils.isNoneBlank(v.getAs[String]("idfa")) => "ID:" + v.getAs[String]("idfa")
      case v if StringUtils.isNoneBlank(v.getAs[String]("openudid")) => "OD:" + v.getAs[String]("openudid")
      case v if StringUtils.isNoneBlank(v.getAs[String]("androidid")) => "AOD:" + v.getAs[String]("androidid")
      case v if StringUtils.isNoneBlank(v.getAs[String]("imeimd5")) => "IMM:" + v.getAs[String]("imeimd5")
      case v if StringUtils.isNoneBlank(v.getAs[String]("macmd5")) => "MCM:" + v.getAs[String]("macmd5")
      case v if StringUtils.isNoneBlank(v.getAs[String]("idfamd5")) => "IDM:" + v.getAs[String]("idfamd5")
      case v if StringUtils.isNoneBlank(v.getAs[String]("openudidmd5")) => "ODM:" + v.getAs[String]("openudidmd5")
      case v if StringUtils.isNoneBlank(v.getAs[String]("androididmd5")) => "AODM:" + v.getAs[String]("androididmd5")
      case v if StringUtils.isNoneBlank(v.getAs[String]("imeisha1")) => "IMS:" + v.getAs[String]("imeisha1")
      case v if StringUtils.isNoneBlank(v.getAs[String]("macsha1")) => "MCS:" + v.getAs[String]("macsha1")
      case v if StringUtils.isNoneBlank(v.getAs[String]("idfasha1")) => "IDS:" + v.getAs[String]("idfasha1")
      case v if StringUtils.isNoneBlank(v.getAs[String]("openudidsha1")) => "ODS:" + v.getAs[String]("openudidsha1")
      case v if StringUtils.isNoneBlank(v.getAs[String]("androididsha1")) => "AODS:" + v.getAs[String]("androididsha1")
    }
  }

  val hasNeedOneUserId =
    """
      |imei!='' or mac != '' or idfa != '' or openudid !='' or androidid !='' or
      |imeimd5!='' or macmd5 != '' or idfamd5 != '' or openudidmd5 !='' or androididmd5 !='' or
      |imeisha1!='' or macsha1 != '' or idfasha1 != '' or openudidsha1 !='' or androididsha1 !=''
    """.stripMargin

  def getUserIdAll(v: Row): List[String] = {
    var list = List[String]()
    if (StringUtils.isNoneEmpty(v.getAs[String]("imei"))) {
      list :+= "IM:" + v.getAs[String]("imei")
    }
    if (StringUtils.isNoneEmpty(v.getAs[String]("mac"))) {
      list :+= "MC:" + v.getAs[String]("mac")
    }
    if (StringUtils.isNoneEmpty(v.getAs[String]("idfa"))) {
      list :+= "ID:" + v.getAs[String]("idfa")
    }
    if (StringUtils.isNoneEmpty(v.getAs[String]("openudid"))) {
      list :+= "OD:" + v.getAs[String]("openudid")
    }
    if (StringUtils.isNoneEmpty(v.getAs[String]("androidid"))) {
      list :+= "AOD:" + v.getAs[String]("androidid")
    }
    if (StringUtils.isNoneEmpty(v.getAs[String]("imeimd5"))) {
      list :+= "IMM:" + v.getAs[String]("imeimd5")
    }
    if (StringUtils.isNoneEmpty(v.getAs[String]("macmd5"))) {
      list :+= "MCM:" + v.getAs[String]("macmd5")
    }
    if (StringUtils.isNoneEmpty(v.getAs[String]("idfamd5"))) {
      list :+= "IDM:" + v.getAs[String]("idfamd5")
    }
    if (StringUtils.isNoneEmpty(v.getAs[String]("openudidmd5"))) {
      list :+= "ODM:" + v.getAs[String]("openudidmd5")
    }
    if (StringUtils.isNoneEmpty(v.getAs[String]("androididmd5"))) {
      list :+= "AODM:" + v.getAs[String]("androididmd5")
    }
    if (StringUtils.isNoneEmpty(v.getAs[String]("imeisha1"))) {
      list :+= "IMS:" + v.getAs[String]("imeisha1")
    }
    if (StringUtils.isNoneEmpty(v.getAs[String]("macsha1"))) {
      list :+= "MCS:" + v.getAs[String]("macsha1")
    }
    if (StringUtils.isNoneEmpty(v.getAs[String]("idfasha1"))) {
      list :+= "IDS:" + v.getAs[String]("idfasha1")
    }
    if (StringUtils.isNoneEmpty(v.getAs[String]("openudidsha1"))) {
      list :+= "ODS:" + v.getAs[String]("openudidsha1")
    }
    if (StringUtils.isNoneEmpty(v.getAs[String]("androididsha1"))) {
      list :+= "AODS:" + v.getAs[String]("androididsha1")
    }
    list
  }
}
