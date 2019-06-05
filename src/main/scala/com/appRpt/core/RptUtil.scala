package com.appRpt.core

object RptUtil {
  /**
    * 功能描述:
    * 〈 请求数 〉
    *
    * @param requestMode
    * @param progressMode
    * @return:scala.collection.immutable.List <java.lang.Object>
    * @since: 1.0.0
    * @Author:SiXiang
    * @Date: 2019/6/5 13:57
    */
  def request(requestMode: Int, progressMode: Int): List[Double] = {
    var list = List[Double](0, 0, 0)
    if (requestMode == 1 && progressMode >= 1) {
      list = List[Double](1, 0, 0)
      if (progressMode == 2) {
        list = List[Double](1, 1, 0)
      }
      if (progressMode == 3) {
        list = List[Double](1, 1, 1)
      }
      list
    } else {
      list
    }
  }

  /**
    * 功能描述:
    * 〈 竞价数 〉
    *
    * @param isEffective
    * @param isBilling
    * @param isBid
    * @param isWin
    * @param adOrderId
    * @return:scala.collection.immutable.List <java.lang.Object>
    * @since: 1.0.0
    * @Author:SiXiang
    * @Date: 2019/6/5 14:13
    */
  def bidding(isEffective: Int, isBilling: Int, isBid: Int, isWin: Int, adOrderId: Int): List[Double] = {
    var list = List[Double](0, 0)
    if (isEffective == 1 && isBilling == 1) {
      if (isBid == 1) {
        list = List[Double](1, 0)
      }
      if (isWin == 1 && adOrderId != 0) {
        list = List[Double](1, 1)
      }
      list
    } else {
      list
    }
  }

  /**
    * 功能描述:
    * 〈 展示数、点击数 〉
    *
    * @param requestMode
    * @param isEffective
    * @return:scala.collection.immutable.List <java.lang.Object>
    * @since: 1.0.0
    * @Author:SiXiang
    * @Date: 2019/6/5 17:32
    */
  def showsAndClicks(requestMode: Int, isEffective: Int): List[Double] = {
    var list = List[Double](0, 0)
    if (requestMode == 2 && isEffective == 1) {
      list = List[Double](1, 0)
    }
    if (requestMode == 3 && isEffective == 1) {
      list = List[Double](1, 1)
    }
    list
  }

  /**
    * 功能描述:
    * 〈 DSP广告消费 、DSP广告成本 〉
    *
    * @param isEffective
    * @param isBilling
    * @param isWin
    * @param winPrice
    * @param adPayment
    * @return:scala.collection.immutable.List <java.lang.Object>
    * @since: 1.0.0
    * @Author:SiXiang
    * @Date: 2019/6/5 17:43
    */

  def dsp(isEffective: Int, isBilling: Int, isWin: Int, winPrice: Double, adPayment: Double): List[Double] = {
    var list = List[Double](0, 0)
    if (isEffective == 1 && isBilling == 1 && isWin == 1) {
      list = List[Double](winPrice / 1000.0, adPayment / 1000.0)
    }
    list
  }

}
