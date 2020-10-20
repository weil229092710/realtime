package com.xuehai.utils

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.{LocationStrategies, KafkaUtils}
import org.apache.spark.{SparkContext, SparkConf}
import org.slf4j.LoggerFactory

/**
  * Created by Administrator on 2019/5/10 0010.
  */
trait Constants {
    val LOG = LoggerFactory.getLogger(this.getClass)
    /**
      * mongoDB
      */
    val mongoHost = PropertiesUtil.getKey("mongo_host")
    val mongoPort = PropertiesUtil.getKey("mongo_port")
    val mongoUser = PropertiesUtil.getKey("mongo_user")
    val mongoPassword = PropertiesUtil.getKey("mongo_password")
    val mongoDB = PropertiesUtil.getKey("mongo_db")
    val mongoCollection = PropertiesUtil.getKey("mongo_collection")

	/**
      * spark
      */
    val sparkConf: SparkConf ={
        // 1；spark集群运行模式
        //val conf = new SparkConf().setAppName("realTime-job").setMaster("local[*]")
        val conf = new SparkConf().setAppName("realTime-job")



        // 2；hive配置
        // hive动态分区功能
        conf.set("hive.exec.dynamic.partition", "true")
        conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
        // 测试环境使用
        //conf.set("fs.defaultFS", "hdfs://xuehai-nn:8020")



        // 3；mongo参数配置
        // 预发布环境：V3.3.6
        conf.set("spark.mongodb.input.uri", "mongodb://%s:%s/%s.%s".format(mongoHost, mongoPort, mongoDB, mongoCollection))

       // conf.set("spark.mongodb.input.uri", "mongodb://localhost:27017/xh_king.pkGameInfo1")


          // 生产环境
       // conf.set("spark.mongodb.input.uri", "mongodb://%s:%s@%s:%s/%s.%s".format(mongoUser, mongoPassword, mongoHost, mongoPort, mongoDB, mongoCollection))

        // 测试环境：需要认证
       // conf.set("spark.mongodb.input.uri", "mongodb://%s:%s@%s:%s/%s.%s?authSource=admin".format(mongoUser, mongoPassword, mongoHost, mongoPort, mongoDB, mongoCollection))

        // 开发环境：无需认证，且版本低于3.0
        // conf.set("spark.mongodb.input.uri", "mongodb://%s:%s/%s.%s".format(mongoHost, mongoPort, mongoDB, mongoCollection))
        // 解决MongoDB version < 3.2 抛异常的问题
        // conf.set("spark.mongodb.input.partitioner","MongoShardedPartitioner").set("spark.mongodb.input.partitionerOptions.shardkey","_id")




        // 4；kafka参数配置
        conf.set("spark.streaming.kafka.maxRatePerPartition", "3000") //一次读取单个partition最大数据量

        conf
    }

    /**
      * Streaming实时读取kafka数据，如果指定offset，则从指定位置读取kafka数据，否则根据kafka参数auto.offset.reset读取
      *
      * @param ssc sparkStreaming
      * @param topics topic
      * @param kafkaParams kafka参数
      * @param offsets 指定读取kafka位置
      * @return
      */
    def getCreateDirectStream(ssc: StreamingContext, topics: Iterable[String], kafkaParams: Map[String, Object], offsets: Map[TopicPartition, Long] = null): InputDStream[ConsumerRecord[String, String]] ={
        if(null == offsets) return KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, Subscribe[String, String](topics, kafkaParams))

        //TODO
        KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, Subscribe[String, String](topics, kafkaParams))
        //KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, Subscribe[String, String](topics, kafkaParams, offsets))
    }

	/**
      * kafka
      */
    val brokerList = PropertiesUtil.getKey("brokerList")

    val kafkaParams: Map[String, Object] = Map(
        "bootstrap.servers" -> brokerList,
        "auto.offset.reset" -> "latest", // latest, earliest, none
        "enable.auto.commit" -> "false",
        "group.id" -> PropertiesUtil.getKey("kafkaGroupId"),
        "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
        "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    /**
      * redis
      */
    val host = PropertiesUtil.getKey("host")
    val port = PropertiesUtil.getKey("port").toInt
    val db = PropertiesUtil.getKey("db").toInt
    val password = PropertiesUtil.getKey("password")
    val MAX_ACTIVE = 1024
    val MAX_IDLE = 200
    val MAX_WAIT = 10000000
    val TIMEOUT = 10000000
    val TEST_ON_BORROW = true
    val listKey = PropertiesUtil.getKey("listKey")


    /**
      * mysql
      */
    val mysqlHost = PropertiesUtil.getKey("mysql_host")
    val mysqlPort = PropertiesUtil.getKey("mysql_port")
    val mysqlUser = PropertiesUtil.getKey("mysql_user")
    val mysqlPassword = PropertiesUtil.getKey("mysql_password")
    val mysqlDB = PropertiesUtil.getKey("mysql_db")
    val mysqlUtilsUrl = "jdbc:mysql://%s:%s/%s?autoReconnect=true".format(mysqlHost, mysqlPort, mysqlDB)


    /**
      * 钉钉机器人
      */
    val DingDingUrl = PropertiesUtil.getKey("dingding_url")
}