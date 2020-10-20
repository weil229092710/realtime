package com.xuehai.project

import java.sql.ResultSet
import com.alibaba.fastjson.{JSONObject, JSON}
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.rdd.MongoRDD
import com.xuehai.utils._
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.bson.Document

import scala.collection.mutable.ListBuffer

/**
  * Created by root on 2019/10/18.
  */
object ha extends Constants{
  /**
    * kafka基本配置
    */
  val topic = PropertiesUtil.getKey("pk_topic")
  val topicSet = topic.split(",").toSet
  val head = PropertiesUtil.getKey("pk_head") //项目前缀，在redis中区分每个项目的key



  /**
    * 读取该赛季所有数据，更新每个user的pk数据，然后调用updateRank()方法更新榜单
    *

    */
  def main(args: Array[String]): Unit = {
    def sparkMongo(sc: SparkContext): Unit ={
      // 读取当前赛季
      val season = RedisUtil.getString("BIGDATA_KING_PK:CURRENT_SEASON")
      println("beginning read season %s mongo data ........".format(season))
      //读取当前赛季的数据（赛季是根据redis中的为准）
      val mongoRdd: MongoRDD[Document] = MongoSpark.load(sc).withPipeline(Seq(Document.parse("{$match:{season:{$eq:%s}}}".format(season))))
      println(mongoRdd.count())
      val sql = "SELECT student_id, school_id,gradeId FROM `fact_student_info_copy`"

      val results: ResultSet = MysqlUtils.select(sql)

      val docRdd = mongoRdd.map(doc => {
        // {"season": 1,"gameId":2,"gradeId":91,"schoolId":2,"classIds":[1,2],"userId":7,"subjectId":1,"star":1,"victory":1,"time":1563811200000}
        // 数据转换：Document -> (season:gradeId, userId, gameId, subjectId, star, victory, time)
        val sql = "SELECT student_id, school_id,gradeId FROM `fact_student_info_copy` where studentId="+doc.getInteger("studentId")
        val results: ResultSet = MysqlUtils.select(sql)
        while(results.next()){
          val gradeId1 = results.getInt(3)
        }

        val season = doc.getInteger("season")
        val gameId = doc.get("_id").toString
        val gradeId = doc.getInteger("grade")
        val userId = doc.getInteger("studentId")
        val subjectId = doc.getInteger("subjectId")
        val star = doc.getInteger("danChange")
        val victory = doc.getInteger("pkResult")
        val time = doc.getLong("createTime")

        ("%s:%s".format(season, gradeId), userId, gameId, subjectId, star, victory, time)
      })
      docRdd.count()

    }
  }











}
