package com.utils

/**
  * 将数据转换类型
  */
object String2IntOrDouble {
  // 转换Int
  def toInt(str:String):Int={
    try{
      str.toInt
    }catch {
      case _:Exception =>0
    }
  }
  // 转换Double
  def toDouble(str:String):Double={
    try{
      str.toDouble
    }catch {
      case _:Exception =>0
    }
  }
}
