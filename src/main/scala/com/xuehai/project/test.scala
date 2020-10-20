package com.xuehai.project

import com.xuehai.utils.{Constants, RedisUtil, Utils}
import org.apache.spark.sql.SaveMode

import scala.collection.mutable.ListBuffer

object test extends  Constants{
  def main(args: Array[String]): Unit = {
    val values: ListBuffer[String] = ListBuffer()
    var x="sdf"
    try{
      var valueList: String = RedisUtil.getString(x)
      if (valueList == null) {
        valueList = RedisUtil.getString(Utils.lastGrade(x))
        RedisUtil.delKey(Utils.lastGrade(x))
      }
      values.append(valueList)

    }catch {
      case e: Exception => {

        LOG.error("数据导入到mysql出错！"+e.printStackTrace())
      }
    }

  }

}
