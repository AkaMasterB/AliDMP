package com.common

trait Tags {
  def makeTags(args: Any*): List[(String, Int)]
}
