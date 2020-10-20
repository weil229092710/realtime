package com.xuehai.utils

import com.dingtalk.api.DefaultDingTalkClient
import com.dingtalk.api.request.OapiRobotSendRequest
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}
import org.apache.kafka.common.TopicPartition
import scala.collection.JavaConverters._
/**
  * Created by Administrator on 2019/5/23 0023.
  */
object Utils extends Constants{
    /**
      * null 转"null"，非空则直接转String，防止空指针异常
      *
      * @return
      */
    def null2Str(x: Any): String ={
        if(x == null) return ""
        else x.toString
    }

    def null2Double(x: Any): Double ={
        try{
            x.toString.toDouble
        }catch {
            case e:Exception => 0.toDouble
        }
    }

    /**
      * null 转0，计算kafka的offset使用
      *
      * @param x
      * @return
      */
    def null20(x: Any): Int ={
        if(x == null) return 0
        else x.toString.toInt
    }

    /**
      * null 转负数 -1，计算kafka的offset使用
      *
      * @param x
      * @return
      */
    def null2Negative(x: Any): Long ={
        if(x == null) return -1
        else x.toString.toLong
    }
    /**
      * 读取redis，获取kafka的起始offset，记得需要加1
      *
      * @return
      */
    def getOffSets(topic: String, item: String): Map[TopicPartition, Long] = {
        if(topic=="xhjvm-service" || topic=="nginx" || topic=="assist-service"){
            //null
            Map(
                new TopicPartition(topic, 0) -> (null2Negative(RedisUtil.getString(item + ":partition0"))+1)
            )
        }else{
            Map(
                new TopicPartition(topic, 0) -> (null2Negative(RedisUtil.getString(item + ":partition0"))+1),
                new TopicPartition(topic, 1) -> (null2Negative(RedisUtil.getString(item + ":partition1"))+1),
                new TopicPartition(topic, 2) -> (null2Negative(RedisUtil.getString(item + ":partition2"))+1)
            )
        }
    }



    /**
      * 查询上个年级的key 例如1:32:36368 改为1：31:36368
      *
      * @param x
      * @return
      */
    def lastGrade(x: String): String ={
        val strings = x.split(":")
        strings(1)  =strings(1) match {
            case "11"=>"11"
            case "21"=>"16"
            case "31"=>"23"
            case _ =>(strings(1).toInt-1).toString
        }
        val str = "%s:%s:%s".format(strings(0),strings(1),strings(2))
        str
    }

    /**
      * 钉钉机器人消息推送
      *
      * @param user "all"-@所有人，"18810314189,13724612033"-@指定的人，以逗号分割
      * @param massage 推送的消息
      */
    def dingDingRobot(user: String, massage: String): Unit ={
        val text = new OapiRobotSendRequest.Text()
        text.setContent(massage)

        val at = new OapiRobotSendRequest.At()
        if(user=="all"){
            at.setIsAtAll("true")
        }else{
            at.setAtMobiles(user.split(",").toList.asJava)
        }

        val request = new OapiRobotSendRequest()
        request.setMsgtype("text")
        request.setText(text)
        request.setAt(at)

        val client = new DefaultDingTalkClient(DingDingUrl)
        client.execute(request)
    }

    def resetKafkaOffset(): Unit ={
        //val brokerList= "192.168.5.85:9092,192.168.5.86:9092,192.168.5.87:9092"
        //val topic = "mongo-kafka"

        val topic = PropertiesUtil.getKey("pk_topic")
        import java.util.Properties
        val kafkaProp = new Properties()
        kafkaProp.put("bootstrap.servers", brokerList)
        kafkaProp.put("acks", "1")
        kafkaProp.put("retries", "3")
        kafkaProp.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        kafkaProp.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        val producer = new KafkaProducer[String, String](kafkaProp)
        println("partition0")
        val record0 = new ProducerRecord[String, String](topic, 0, "key", "000")
        producer.send(record0)

        println("partition1")
        val record1 = new ProducerRecord[String, String](topic, 1, "key", "111")
        producer.send(record1)

        println("partition2")
        val record2 = new ProducerRecord[String, String](topic, 2, "key", "222")
        producer.send(record2)

        producer.close()
    }


    def main(args: Array[String]) {
        // dingDingRobot("18810314189", "测试")

        //new TopicPartition("nginx", 0) -> (null2Negative("6456379730")+1)

        //resetKafkaOffset()

        println(password)
    }
}
