package com.xuehai.project

import java.util

import com.xuehai.utils.RedisUtil

import scala.collection.mutable.{ListBuffer, Set}

object fg {
  def main(args: Array[String]): Unit = {
   // println(lastGrade("1:31:36368"))
    var x=""
    val data = x.split(",")
    (data(0), data(1), data(7).toInt, data(8).toLong)

  }

//  def lastGrade(x: String): String ={
//    val strings = x.split(":")
//    strings(1)  =strings(1) match {
//      case "11"=>"11"
//      case "21"=>"16"
//      case "31"=>"23"
//      case _ =>(strings(1).toInt-1).toString
//    }
//    val str = "%s:%s:%s".format(strings(0),strings(1),strings(2))
//    str
//  }


}
