import com.xuehai.utils.{DateUtil, PropertiesUtil}
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.{LocationStrategies, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by root on 2019/12/18.
  */
object test2 {
	def main(args: Array[String]) {
//		val topic = "assist-service"
		val topic = "BIGDATA-KING-PK"

		val topics = topic.split(",").toSet
		val kafkaParams: Map[String, Object] = Map(
			"bootstrap.servers" -> "192.168.5.85:9092,192.168.5.86:9092,192.168.5.87:9092",
			"auto.offset.reset" -> "latest",
			"enable.auto.commit" -> "false",
			"group.id" -> PropertiesUtil.getKey("kafkaGroupId"),
			"key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
			"value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
		)

		val conf = new SparkConf().setAppName("realTime-job1")
		conf.setMaster("local[*]")
		val sc = SparkContext.getOrCreate(conf)
		sc.setLogLevel("ERROR")

		val ssc = StreamingContext.getActiveOrCreate(() => new StreamingContext(sc, Seconds(3)))

		val directStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, Subscribe[String, String](topics, kafkaParams))
		directStream.foreachRDD(rdd => {
			println(rdd.count())
		})

		ssc.start()
		ssc.awaitTerminationOrTimeout(DateUtil.getTomorrowHourMilliseconds(3))
		ssc.stop(false, true)

	}
}
