package com.xuehai.project

import java.sql.ResultSet

import com.alibaba.fastjson.{JSON, JSONObject}
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
object PKItem1 extends Constants {
  /**
    * kafka基本配置
    */
  val topic = PropertiesUtil.getKey("pk_topic")
  val topicSet = topic.split(",").toSet
  val head = PropertiesUtil.getKey("pk_head") //项目前缀，在redis中区分每个项目的key


  /**
    * 读取该赛季所有数据，更新每个user的pk数据，然后调用updateRank()方法更新榜单
    *
    * @param sc
    */

  def sparkMongo(sc: SparkContext): Unit = {
    // 读取当前赛季
    val season = RedisUtil.getString("BIGDATA_KING_PK:CURRENT_SEASON")
    println("beginning read season %s mongo data ........".format(season))
    //读取当前赛季的数据（赛季是根据redis中的为准）
    val mongoRdd: MongoRDD[Document] = MongoSpark.load(sc).withPipeline(Seq(Document.parse("{$match:{season:{$eq:%s}}}".format(season))))
    println(mongoRdd.count())

    //val sql = "SELECT student_id, school_id,gradeId FROM `fact_student_info_distinct_daily`"
    val sql = "SELECT student_id, school_id,gradeId FROM `fact_student_info_copy`"

    val results: ResultSet = MysqlUtils.select(sql)

    val userInfoJson = JSON.parseObject("{}")
    while(results.next()){
      val userId = results.getInt(1)
      val schoolId = results.getInt(2)

      val gradeId = results.getInt(3)
      userInfoJson.put(userId.toString, schoolId.toString+"*/"+gradeId)
    }


    val docRdd = mongoRdd.map(doc => {
      // {"season": 1,"gameId":2,"gradeId":91,"schoolId":2,"classIds":[1,2],"userId":7,"subjectId":1,"star":1,"victory":1,"time":1563811200000}
      // 数据转换：Document -> (season:gradeIdD, userId, gameId, subjectId, star, victory, time)
      val season = doc.getInteger("season")
      val gameId = doc.get("_id").toString
      val userId = doc.getInteger("studentId")
      val subjectId = doc.getInteger("subjectId")
      val star = doc.getInteger("danChange")
      val victory = doc.getInteger("pkResult")
      val time = doc.getLong("createTime")
      var gradeId = doc.getInteger("grade")
      var schoolId = doc.getInteger("schoolId")


      if(Utils.null2Str(userInfoJson.getString(userId.toString)).length>0) {
         schoolId = Utils.null2Str(userInfoJson.getString(userId.toString)).split("\\*/")(0).toInt
         gradeId = Utils.null2Str(userInfoJson.getString(userId.toString)).split("\\*/")(1).toInt
      }

      (season, userId, gameId, subjectId, star, victory, time, gameId, gradeId, schoolId)

    })

    docRdd.groupBy(_._1) // season
      .foreach(x2 => {
        // 一个年级所有数据
        var seasonAndGradeId = " "
        // 一个用户所有数据
        val season = x2._2.toList.head._1
        val gradeId = x2._2.toList.head._9
        seasonAndGradeId = "%s:%s".format(season, gradeId)
        val subjectIds = RedisUtil.getString("BIGDATA_KING_PK:%s".format(seasonAndGradeId))

        if (null == subjectIds || subjectIds.length < 1) { //如果没有科目ID，则不需要计算
          println("seasonAndGradeId-%s: No subjectIds, Please check!!!!!!!!!!".format(seasonAndGradeId))
        } else {
          val keyValues: ListBuffer[String] = ListBuffer()
          val allKey: ListBuffer[String] = ListBuffer()

          x2._2.groupBy(_._2).foreach(x3 => { // userId
            try {
              // 一个学生的全部数据
              val userId = x3._1
              val schooldId = x3._2.toList.head._10
              val season = x3._2.toList.head._1
              val gradeId = x3._2.toList.head._9
              seasonAndGradeId = "%s:%s".format(season, gradeId)
              val subjectIds = RedisUtil.getString("BIGDATA_KING_PK:%s".format(seasonAndGradeId))

              if (null != schooldId) { //如果schoolId为null，则该学生被删除，不进行计算
                // ("%s:%s".format(season, gradeId), userId, gameId, subjectId, star, victory, time)
                val values = x3._2.toList.sortBy(_._7)

                val gameCount = values.size
                val victoryCount = values.count(_._6 == 1)
                val lostCount = values.count(_._6 == 2)
                val netVictoryCount = victoryCount - lostCount

                val subjectIdAndStarAndTime = subjectIds.split(",").map(subjectId => {
                  // 计算单科的star、time
                  var star = 0
                  var time = 0L
                  values.foreach(x4 => {
                    if (x4._4 == subjectId.toInt) {
                      star += x4._5.toInt
                      if (star < 0) star = 0 //如果分数为负，则置为0
                      time = x4._7 //单科最新时间
                    }
                  })
                  (subjectId, star, time)
                })


                val subjectIdsAndStarAndTime = subjectIdAndStarAndTime.map(x5 => "%s:%s:%s".format(x5._1, x5._2, x5._3)).mkString("&")
                val minStarAndTime = subjectIdAndStarAndTime.sortBy(x6 => (x6._2, x6._3)).head
                val minStar = minStarAndTime._2

                val minStarTime = minStarAndTime._3
                val time = values.last._7

                val key = "%s:%s".format(seasonAndGradeId, userId)
                // schoolId,userId,gameCount,victoryCount,lostCount,netVictoryCount,subjectId1:star:time&subjectId2:star:time,minStar,minStarTime,lastTime
                val value = "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s".format(schooldId, userId, gameCount, victoryCount, lostCount, netVictoryCount, subjectIdsAndStarAndTime, minStar, minStarTime, time)

                keyValues.append(key, value)
                allKey.append("%s:%s".format(seasonAndGradeId, userId.toString))


              }
            } catch {
              case e: NullPointerException => {}
              case e: Exception => e.printStackTrace()
            }
          })

          // 本赛季、本年级的所有用户数据
          // 更新所有用户的PK数据
          keyValues.sliding(50, 50).foreach(list => {
            try {
              RedisUtil.putmString(list.toList)
            } catch {
              case e: Exception => {
                println("mongo data : update user redis massage error!!!!!!!!!!")
                e.printStackTrace()
              }
            }
          })


          val allKey1: ListBuffer[String] = ListBuffer()
          val list1: ListBuffer[String] = ListBuffer()
          x2._2.groupBy(_._9).foreach(
            byseacon=>{
              val season = byseacon._2.toList.head._1
              val gradeId = byseacon._2.toList.head._9
              seasonAndGradeId = "%s:%s".format(season, gradeId)
               byseacon._2.toList.foreach(
                 x=>
                   allKey1.append("%s:%s".format(seasonAndGradeId, x._2.toString))
               )
              // 存储本赛季、本年级所有的key
              RedisUtil.putString(seasonAndGradeId + ":userIds", allKey1.distinct.mkString(","))
              allKey1.clear()

              // 存储本赛季、本年级所有的gameId
              byseacon._2.map(x7 => x7._3.toString).toList.distinct.foreach(list => {
                list1.append(list)
              })
              RedisUtil.putmSet(seasonAndGradeId + ":gameIds", list1.toList)
               list1.clear()
            })





          // 更新榜单
          updateRank(allKey.toList)
        }
      })
  }


  /**
    * sparkStreaming实时计算函数
    *
    * @param ssc StreamingContext
    *
    */
  def sparkStreaming(ssc: StreamingContext) = {
    val directStream = getCreateDirectStream(ssc, topicSet, kafkaParams, Utils.getOffSets(topic, head))

    directStream.foreachRDD(rdd => {

      rdd.foreachPartition(partition => {
        val all: List[(Int, Long, String)] = partition.map(line => {
          val partition = line.partition()
          val offSet = line.offset()
          val value = line.value()
          (partition, offSet, value)
        }).toList

        if (all.nonEmpty) {
          try {
            val keys: List[String] = perPartitionData(all) // 1、更新用户信息
            val x = all.maxBy(_._2)

            RedisUtil.putString("%s:partition%s".format(head, x._1), x._2.toString) // 2、手动维护kafka的offset

            updateRank(keys) // 3、更新排行榜操作
          } catch {
            case e: Exception => {
              println("kafka data error!!!!!!!")
              e.printStackTrace()
            }
          }
        }
      })
    })

  }


  /**
    * 对每个partition的数据进行清洗处理，实时更新每个用户的数据
    *
    * @param all 每个partition的全部数据
    * @return 每个partition的全部key
    */
  def perPartitionData(all: List[(Int, Long, String)]): List[String] = {
    // {"season": 1,"gameId":2,"pkId":1,"gradeId":91,"schoolId":2,"classIds":[1,2],"userId":7,"subjectId":1,"star":1,"victory":1,"time":1563811200000}
    val jsons: List[JSONObject] = all.map(x => {
      try {
        val json = JSON.parseObject(x._3)
        val season = json.getString("season")
        val gradeId = json.getString("gradeId")
        val gameId = json.getString("gameId")

        // 判断本条数据是否处理过
        if (RedisUtil.putSet("%s:%s:gameIds".format(season, gradeId), gameId) == "1") {
          json
        } else {
          JSON.parseObject("{}")
        }
      } catch {
        case e: Exception => JSON.parseObject("{}")
      }
    }).filter(_.size() == 11) // 过滤脏数据：过滤掉空字符串和json长度不等于11


    // 所有变动的key
    val keys: List[String] = jsons.map(json => {
      val season = json.getString("season")
      val gradeId = json.getString("gradeId")
      val userId = json.getString("userId")

      "%s:%s:%s".format(season, gradeId, userId)
    }).distinct


    // 读取每个用户存储在redis的值schoolId,userId,gameCount,victoryCount,lostCount,netVictoryCount,subjectId1:star:time&subjectId2:star:time,minStar,minStarTime,lastTime
    val values: ListBuffer[String] = ListBuffer()
    //		keys.sliding(100, 100).foreach(keyList => {
    //
    //			val valueList: List[String] = RedisUtil.getmString(keyList)
    //			values.++=(valueList)
    //		})
		keys.foreach {
			x =>
				var valueList: String = RedisUtil.getString(x)
				if (valueList == null) {
					valueList = RedisUtil.getString(Utils.lastGrade(x))
				}
				values.append(valueList)
		}

    val keyValues: ListBuffer[String] = ListBuffer()

    // 更新每个userID的数据
    for (i <- 0 until jsons.size) {
      //kafkaJson -> {"season": 1,"gameId":2,"pkId":2"gradeId":91,"schoolId":2,"classIds":[1,2],"userId":7,"subjectId":1,"star":1,"victory":1,"time":1563811200000}
      try {
        val kafkaJson = jsons(i)

        // 解决同一批数据中，一个用户有多条数据的问题，已经把key做了去重，所以一个用户只有一条数据
        val key = kafkaJson.getString("season") + ":" + kafkaJson.getString("gradeId") + ":" + kafkaJson.getString("userId")
        val index = keys.indexOf(key)

        val time = kafkaJson.getLong("time")


        val singleArray = values(index) match {
          case null => { //第一次把基础信息添加进去
            val array = new Array[String](10)
            val season = kafkaJson.getString("season")
            val gradeId = kafkaJson.getString("gradeId")

            array(0) = kafkaJson.getString("schoolId") // schoolId
            array(1) = kafkaJson.getString("userId") // userId
            array(2) = "0" // gameCount
            array(3) = "0" // victoryCount
            array(4) = "0" // lostCount
            array(5) = "0" // netVictoryCount

            // subjectId1:0:1563811200000&subjectId2:0:1563811200000&subjectId3:0:1563811200000
            try {
              // 读取本赛季下，该年级的所有科目ID，进行新用户数据的初始化
              val subjectIds = RedisUtil.getString("BIGDATA_KING_PK:%s:%s".format(season, gradeId))
              array(6) = subjectIds.split(",").map(_ + ":0:" + time).mkString("&")
            } catch {
              case e: Exception => {
                array(6) = ""
              }
            }

            array(7) = "0" // minStar
            array(8) = time.toString // minStarTime
            array(9) = time.toString // lastTime

            array
          }
          case _ => {
            // 更新学校和班级，防止中途学校和班级变动
            val array: Array[String] = values(index).split(",")
            array(0) = kafkaJson.getString("schoolId")
            array
          }
        }

        val subjectId = kafkaJson.getString("subjectId")
        val star = kafkaJson.getInteger("star")
        val subjects = singleArray(6).split("&")
        for (i <- 0 until subjects.size) {
          if (subjects(i).startsWith(subjectId + ":")) {
            // 只有当subjectId属于该赛季、该年级的科目时，才计算

            // 1；总场数
            singleArray(2) = (singleArray(2).toInt + 1).toString

            // 2；胜场数、失败场数、净胜场数
            // victory：0-平，1-胜，2-负
            val victory = kafkaJson.getInteger("victory")
            var victoryCount = singleArray(3).toInt
            var lostCount = singleArray(4).toInt
            if (victory == 1) {
              victoryCount += 1
            } else if (victory == 2) {
              lostCount += 1
            }
            singleArray(3) = victoryCount.toString
            singleArray(4) = lostCount.toString
            singleArray(5) = (victoryCount - lostCount).toString


            // 3；科目的星数和时间戳
            val count = subjects(i).split(":")(1).toInt + star
            if (count < 0) { // subjectId:star，如果科目分数小于0，则置为0
              subjects(i) = subjectId + ":0:" + time
            } else {
              subjects(i) = subjectId + ":" + count + ":" + time
            }

            singleArray(6) = subjects.mkString("&")

            // 4；取所有科目中最小的星数值
            val minStarAndTime = singleArray(6).split("&").map(x => (x.split(":")(1).toInt, x.split(":")(2).toLong)).sortBy(x => (x._1, x._2)).head
            singleArray(7) = minStarAndTime._1.toString
            singleArray(8) = minStarAndTime._2.toString
            singleArray(9) = time.toString
          }
        }

        // 解决同一批数据中，一个用户有多条数据的问题：如果该用户信息发生改变，则把新值赋给这个用户
        values(index) = singleArray.mkString(",")

        val index2 = keyValues.indexOf(keys(index))
        if (index2 == -1) {
          keyValues.append(keys(index), singleArray.mkString(","))
        } else {
          keyValues.update(index2 + 1, singleArray.mkString(","))
        }

      } catch {
        case e: Exception => {
          println("update user json data error!!!!!!!!!!")
          e.printStackTrace()
        }
      }
    }

    // 更新后的数据批量插入redis
    keyValues.sliding(100, 100).foreach(list => {
      try {
        RedisUtil.putmString(list.toList)
      } catch {
        case e: Exception => {
          println("setmString: update user redis massage error!!!!!!!!!!")
          e.printStackTrace()
        }
      }
    })

    keys
  }

  /**
    * 更新每个平台的段位榜、胜场榜  seasonAndGrade
    *
    * @param keys 每个partition的全部key
    */
  def updateRank(keys: List[String]): Unit = {
    // "%s:%s:%s".format(season, gradeId, userId)
    keys.map(_.split(":").slice(0, 2)).map(_.mkString(":")).distinct.foreach(seasonAndGrade => {
      val redisUserIds: List[String] = Utils.null2Str(RedisUtil.getString(seasonAndGrade + ":userIds")).split(",").toList.filterNot(_ == "")
      val keysUserIds: List[String] = keys.filter(_.startsWith(seasonAndGrade))
      val allUserIds: List[String] = redisUserIds.:::(keysUserIds).distinct

      if (allUserIds.size > redisUserIds.size) { // 如果有新的userId，则存储到redis
        RedisUtil.putString(seasonAndGrade + ":userIds", allUserIds.mkString(","))
      }

      val values: ListBuffer[String] = ListBuffer()
      allUserIds.sliding(100, 100).foreach(keyList => {
        val valueList: List[String] = RedisUtil.getmString(keyList)
        values.++=(valueList)

      })

      // values: schoolId,userId,gameCount,victoryCount,lostCount,netVictoryCount,subjectId1:star:time&subjectId2:star:time,minStar,minStarTime,lastTime
      // 1；段位榜
      val starRank = values.map(x => {
        val data = x.split(",")
        (data(0), data(1), data(7).toInt, data(8).toLong) //schoolId, userId, minStar, minStarTime
      }).sortBy(x => (x._3, 2000000000000L - x._4)).reverse.map(x => "%s,%s,%s".format(x._1, x._2, x._3))
      RedisUtil.putString("BIGDATA_KING_PK:STAR:" + seasonAndGrade, starRank.mkString("&"))

      // 2；胜场榜
      val netVictoryCountRank = values.map(x => {
        val data = x.split(",")
        (data(0), data(1), data(2).toInt, data(5).toInt, data(9).toLong) //schoolId, userId, gameCount, netVictoryCount, lastTime
      }).sortBy(x => (x._4, x._3, 2000000000000L - x._5)).reverse.map(x => "%s,%s,%s,%s".format(x._1, x._2, x._3, x._4))
      RedisUtil.putString("BIGDATA_KING_PK:VICTORY:" + seasonAndGrade, netVictoryCountRank.mkString("&"))

      // 3；有变动的年级榜，存储到redis
      RedisUtil.putHash("BIGDATA_KING_PK:MGM", seasonAndGrade, System.currentTimeMillis().toString)
    })
  }
}
