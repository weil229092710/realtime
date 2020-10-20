import com.xuehai.project.{PKItem1, PKItem3}
import com.xuehai.utils
import com.xuehai.utils._
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by root on 2019/6/26 0026.
  */
object AppMain extends Constants{
    def main(args: Array[String]) {
        val sc = SparkContext.getOrCreate(sparkConf)
        sc.setLogLevel("ERROR")

        while(true){
            println("Date: %s".format(DateUtil.getTodayDateStr()))
            val ssc: StreamingContext = new StreamingContext(sc, Seconds(3))

            // 离线任务
            try{
                PKItem3.sparkMongo(sc)   // PK排行榜离线计算
            }catch {
                case e: Exception => {
                    e.printStackTrace()
                   // utils.Utils.dingDingRobot("all", e.getMessage)
                }
            }

            // 实时任务
            try{
                println("reading kafka ......")
                PKItem3.sparkStreaming(ssc)  // PK排行榜实时计算
            }catch {
                case e: Exception => {
                    e.printStackTrace()
                 //   utils.Utils.dingDingRobot("all", e.getMessage)
                }
            }

            ssc.start()
            //ssc.awaitTerminationOrTimeout(DateUtil.getTomorrowHourMilliseconds(3)) //DateUtil.getTomorrowHourMilliseconds(3)
            ssc.awaitTerminationOrTimeout(DateUtil.getTomorrowHourMilliseconds(3))
            ssc.stop(false, true)
        }
    }
}
