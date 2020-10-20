
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Random;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by root on 2019/7/5 0005.
 */
public class kafka_producer {
//    public static String topic = "assist-service";
    public static String topic = "BIGDATA-KING-PK";


    public static String brokerList = "192.168.5.85:9092,192.168.5.86:9092,192.168.5.87:9092";
    public static List<Boolean> victory = new ArrayList<Boolean>();

    public static void main(String[] args) {
        kafka_producer();
//        kafka_test();
    }

    public static void kafka_test(){
        Properties props = new Properties();



        props.put("bootstrap.servers", brokerList);
        props.put("acks", "1");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

//        String nginxStr = "183.247.214.187 17/Oct/2019:17:04:40 +0800 POST /king/api/v3/configs/student/161090/config/info/?sign=74fd7ea33fb245ed937841f264473071&t=1571303120165 HTTP/1.1 0.072 0.057 200 21760 47.110.181.229 \"com.xh.abrustu/v3.20.24.20190927 (SM-P200; android; 9; R22M60089AB)\" 22165 1083 161090 4101 [172.16.50.251:8888] 000000000001207f0000016df67fc013";
        String assistStr = "{\"logType\":\"ERROR\",\"remoteIp\":\"112.15.142.109\",\"sdcardAvailableSpaceSize\":\"5394MB\\r\",\"sdcardTotalSpaceSize\":\"11257MB\\r\",\"crashHappenTime\":\"1571992485764\",\"message\":\"[ ERROR] [2019-10-25 16:40:16.768] com.xhtech.service.assist.infrastructure.log.LogService [29] [XNIO-2 task-56]- logType=ERROR\\nuserId=119245\\ndeviceId=R22K800G00V\\nremoteIp=112.15.142.109\\npackageName=com.xh.arespunc\\r\\nversionName=v1.19.8.20190907\\r\\nversionCode=174\\r\\ncrashHappenTime=1571992485764\\r\\nschoolId=4958\\r\\nsdcardTotalSpaceSize=11257MB\\r\\nsdcardAvailableSpaceSize=5394MB\\r\\ntotalMemory=1891MB\\r\\navailableMemory=1019MB\\r\\nxhcoreVersionName=3.4.18.1\\r\\nBOARD=MSM8916\\r\\nBOOTLOADER=P355CZCU3BRK1\\r\\nBRAND=samsung\\r\\nCPU_ABI=armeabi-v7a\\r\\nCPU_ABI2=armeabi\\r\\nDEVICE=gt5note8ltechn\\r\\nDISPLAY=MMB29M.P355CZCU3BRK1\\r\\nFINGERPRINT=samsung/gt5note8ltezc/gt5note8ltechn:6.0.1/MMB29M/P355CZCU3BRK1:user/release-keys\\r\\nFOTA_INFO=1542709839\\r\\nHARDWARE=qcom\\r\\nHOST=SWDG5220\\r\\nID=MMB29M\\r\\nIS_DEBUGGABLE=false\\r\\nIS_SECURE=false\\r\\nIS_SYSTEM_SECURE=false\\r\\nIS_TRANSLATION_ASSISTANT_ENABLED=false\\r\\nMANUFACTURER=samsung\\r\\nMODEL=SM-P355C\\r\\nPRODUCT=gt5note8ltezc\\r\\nRADIO=unknown\\r\\nSERIAL=1a833263\\r\\nSUPPORTED_32_BIT_ABIS=[Ljava.lang.String;@1bb0720\\r\\nSUPPORTED_64_BIT_ABIS=[Ljava.lang.String;@b8df8d9\\r\\nSUPPORTED_ABIS=[Ljava.lang.String;@e1b429e\\r\\nTAG=Build\\r\\nTAGS=release-keys\\r\\nTIME=1542709280000\\r\\nTYPE=user\\r\\nUNKNOWN=unknown\\r\\nUSER=dpi\\r\\nisOSUpgradeKK2LL=false\\r\\nisLastFail=true\\r\\nX-B3-TraceId=000000000001d1cd0000016e020da390\\r\\nconnectedWifiInfo=SSID: ZTZZ-mrz, BSSID: 70:3a:73:80:ab:af, MAC: 02:00:00:00:00:00, Supplicant state: COMPLETED, RSSI: -49, Link speed: 72Mbps, Frequency: 5805MHz, Net ID: 2, Metered hint: false, score: 60\\r\\nurl=http://192.168.1.100/check.xh\\r\\nrequestMethod=GET\\r\\nrequestHeaders=\\r\\nUserId: 119245\\r\\nSchoolId: 4958\\r\\nUser-Agent: com.xh.arespunc/v1.19.8.20190907 (SM-P355C; android; 6.0.1; R22K800G00V)\\r\\nContent-Type: application/json; charset=UTF-8\\r\\nX-B3-SpanId: 7f8657a4efd70ed4\\r\\nX-B3-TraceId: 000000000001d1cd0000016e020da390\\r\\nAuthorization: Bearer eyJhbGciOiJIUzUxMiJ9.eyJ1aWQiOiIxMTkyNDUiLCJzdWIiOiIzMzAyODEyMDA1MTIyMzUyMjgiLCJjcmVhdGVkIjoxNTcxODk4MDkyNzEzLCJyZXBlYXQiOmZhbHNlLCJhdXRob3JpdHkiOm51bGwsImlzcyI6IjZoU05qV0c2M1hmZVRqU1A5azExVTR6Qlh1RXVLaUpDIiwiZXhwIjoxNTcyNTAyODkyLCJ2ZXJzaW9uIjoiMTE4NzI1Mjc2ODMxMDU1ODgxMSIsInNpZCI6IjBlMzk2NjVkYWJkMzQ1OTg5MGI0YzkzZmM0ZTJlY2JmIn0.3Ay7AXhW2GnHoSzhI4X7PQm7gspkEeegdHFbrShiPGw3PCAhJHkazmibdK0qEyoBGAR7jQ0uSW6FzlEIEbm9Bg\\r\\n\\r\\nrequestBody=null\\r\\nhttpStatusCode=-1\\r\\nresponseHeaders=response is null\\r\\nresponseBody=download response body!\\r\\nconnectionDuration=-1\\r\\ndnsDuration=0\\r\\nhttpEventMessage=http://192.168.1.100/check.xh 1:0.001-callStart;0.003-dnsStart;0.003-dnsEnd;0.004-connectStart;5.011-connectFailed;5.014-callFailed;\\r\\n\\r\\nErrorCode=107004001\\r\\nDescription=无法连接到服务器！\\r\\nErrorMessage=append error message: java.net.SocketTimeoutException: failed to connect to /192.168.1.100 (port 80) after 5000ms\\r\\n\\tat libcore.io.IoBridge.connectErrno(IoBridge.java:169)\\r\\n\\tat libcore.io.IoBridge.connect(IoBridge.java:122)\\r\\n\\tat java.net.PlainSocketImpl.connect(PlainSocketImpl.java:183)\\r\\n\\tat java.net.PlainSocketImpl.connect(PlainSocketImpl.java:452)\\r\\n\\tat java.net.Socket.connect(Socket.java:884)\\r\\n\\tat okhttp3.internal.platform.AndroidPlatform.connectSocket(AndroidPlatform.java:73)\\r\\n\\tat okhttp3.internal.connection.RealConnection.connectSocket(RealConnection.java:245)\\r\\n\\tat okhttp3.internal.connection.RealConnection.connect(RealConnection.java:165)\\r\\n\\tat okhttp3.internal.connection.StreamAllocation.findConnection(StreamAllocation.java:257)\\r\\n\\tat okhttp3.internal.connection.StreamAllocation.findHealthyConnection(StreamAllocation.java:135)\\r\\n\\tat okhttp3.internal.connection.StreamAllocation.newStream(StreamAllocation.java:114)\\r\\n\\tat okhttp3.internal.connection.ConnectInterceptor.intercept(ConnectInterceptor.java:42)\\r\\n\\tat okhttp3.internal.http.RealInterceptorChain.proceed(RealInterceptorChain.java:147)\\r\\n\\tat okhttp3.internal.http.RealInterceptorChain.proceed(RealInterceptorChain.java:121)\\r\\n\\tat okhttp3.internal.cache.CacheInterceptor.intercept(CacheInterceptor.java:93)\\r\\n\\tat okhttp3.internal.http.RealInterceptorChain.proceed(RealInterceptorChain.java:147)\\r\\n\\tat okhttp3.internal.http.RealInterceptorChain.proceed(RealInterceptorChain.java:121)\\r\\n\\tat okhttp3.internal.http.BridgeInterceptor.intercept(BridgeInterceptor.java:93)\\r\\n\\tat okhttp3.internal.http.RealInterceptorChain.proceed(RealInterceptorChain.java:147)\\r\\n\\tat okhttp3.internal.http.RetryAndFollowUpInterceptor.intercept(RetryAndFollowUpInterceptor.java:126)\\r\\n\\tat okhttp3.internal.http.RealInterceptorChain.proceed(RealInterceptorChain.java:147)\\r\\n\\tat okhttp3.internal.http.RealInterceptorChain.proceed(RealInterceptorChain.java:121)\\r\\n\\tat okhttp3.logging.HttpLoggingInterceptor.intercept(HttpLoggingInterceptor.java:213)\\r\\n\\tat okhttp3.internal.http.RealInterceptorChain.proceed(RealInterceptorChain.java:147)\\r\\n\\tat okhttp3.internal.http.RealInterceptorChain.proceed(RealInterceptorChain.java:121)\\r\\n\\tat com.xh.xhcore.common.http.strategy.okhttp.interceptors.RedirectInterceptor.intercept(RedirectInterceptor.kt:13)\\r\\n\\tat okhttp3.internal.http.RealInterceptorChain.proceed(RealInterceptorChain.java:147)\\r\\n\\tat okhttp3.internal.http.RealInterceptorChain.proceed(RealInterceptorChain.java:121)\\r\\n\\tat okhttp3.RealCall.getResponseWithInterceptorChain(RealCall.java:200)\\r\\n\\tat okhttp3.RealCall$AsyncCall.execute(RealCall.java:147)\\r\\n\\tat okhttp3.internal.NamedRunnable.run(NamedRunnable.java:32)\\r\\n\\tat java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1113)\\r\\n\\tat java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:588)\\r\\n\\tat java.lang.Thread.run(Thread.java:818)\\r\\nappend error message: java.lang.Throwable\\r\\n\\tat com.xh.xhcore.common.http.strategy.xh.download.XHDownloadOkHttpProxy$XHDownloadCallbackInC$1.run(XHDownloadOkHttpProxy.java:229)\\r\\n\\tat android.os.Handler.handleCallback(Handler.java:739)\\r\\n\\tat android.os.Handler.dispatchMessage(Handler.java:95)\\r\\n\\tat com.xh.logutils.FireLooper.run(FireLooper.java:76)\\r\\n\\tat android.os.Handler.handleCallback(Handler.java:739)\\r\\n\\tat android.os.Handler.dispatchMessage(Handler.java:95)\\r\\n\\tat android.os.Looper.loop(Looper.java:148)\\r\\n\\tat android.app.ActivityThread.main(ActivityThread.java:7325)\\r\\n\\tat java.lang.reflect.Method.invoke(Native Method)\\r\\n\\tat com.android.internal.os.ZygoteInit$MethodAndArgsCaller.run(ZygoteInit.java:1321)\\r\\n\\tat com.android.internal.os.ZygoteInit.main(ZygoteInit.java:1211)\\r\",\"type\":\"assist-service\",\"versionName\":\"v1.19.8.20190907\\r\",\"body\":\"X-B3-TraceId=000000000001d1cd0000016e020da390\\r\\nconnectedWifiInfo=SSID: ZTZZ-mrz, BSSID: 70:3a:73:80:ab:af, MAC: 02:00:00:00:00:00, Supplicant state: COMPLETED, RSSI: -49, Link speed: 72Mbps, Frequency: 5805MHz, Net ID: 2, Metered hint: false, score: 60\\r\\nurl=http://192.168.1.100/check.xh\\r\\nrequestMethod=GET\\r\\nrequestHeaders=\\r\\nUserId: 119245\\r\\nSchoolId: 4958\\r\\nUser-Agent: com.xh.arespunc/v1.19.8.20190907 (SM-P355C; android; 6.0.1; R22K800G00V)\\r\\nContent-Type: application/json; charset=UTF-8\\r\\nX-B3-SpanId: 7f8657a4efd70ed4\\r\\nX-B3-TraceId: 000000000001d1cd0000016e020da390\\r\\nAuthorization: Bearer eyJhbGciOiJIUzUxMiJ9.eyJ1aWQiOiIxMTkyNDUiLCJzdWIiOiIzMzAyODEyMDA1MTIyMzUyMjgiLCJjcmVhdGVkIjoxNTcxODk4MDkyNzEzLCJyZXBlYXQiOmZhbHNlLCJhdXRob3JpdHkiOm51bGwsImlzcyI6IjZoU05qV0c2M1hmZVRqU1A5azExVTR6Qlh1RXVLaUpDIiwiZXhwIjoxNTcyNTAyODkyLCJ2ZXJzaW9uIjoiMTE4NzI1Mjc2ODMxMDU1ODgxMSIsInNpZCI6IjBlMzk2NjVkYWJkMzQ1OTg5MGI0YzkzZmM0ZTJlY2JmIn0.3Ay7AXhW2GnHoSzhI4X7PQm7gspkEeegdHFbrShiPGw3PCAhJHkazmibdK0qEyoBGAR7jQ0uSW6FzlEIEbm9Bg\\r\\n\\r\\nrequestBody=null\\r\\nhttpStatusCode=-1\\r\\nresponseHeaders=response is null\\r\\nresponseBody=download response body!\\r\\nconnectionDuration=-1\\r\\ndnsDuration=0\\r\\nhttpEventMessage=http://192.168.1.100/check.xh 1:0.001-callStart;0.003-dnsStart;0.003-dnsEnd;0.004-connectStart;5.011-connectFailed;5.014-callFailed;\\r\\n\\r\\nErrorCode=107004001\\r\\nDescription=无法连接到服务器！\\r\\nErrorMessage=append error message: java.net.SocketTimeoutException: failed to connect to /192.168.1.100 (port 80) after 5000ms\\r\\n\\tat libcore.io.IoBridge.connectErrno(IoBridge.java:169)\\r\\n\\tat libcore.io.IoBridge.connect(IoBridge.java:122)\\r\\n\\tat java.net.PlainSocketImpl.connect(PlainSocketImpl.java:183)\\r\\n\\tat java.net.PlainSocketImpl.connect(PlainSocketImpl.java:452)\\r\\n\\tat java.net.Socket.connect(Socket.java:884)\\r\\n\\tat okhttp3.internal.platform.AndroidPlatform.connectSocket(AndroidPlatform.java:73)\\r\\n\\tat okhttp3.internal.connection.RealConnection.connectSocket(RealConnection.java:245)\\r\\n\\tat okhttp3.internal.connection.RealConnection.connect(RealConnection.java:165)\\r\\n\\tat okhttp3.internal.connection.StreamAllocation.findConnection(StreamAllocation.java:257)\\r\\n\\tat okhttp3.internal.connection.StreamAllocation.findHealthyConnection(StreamAllocation.java:135)\\r\\n\\tat okhttp3.internal.connection.StreamAllocation.newStream(StreamAllocation.java:114)\\r\\n\\tat okhttp3.internal.connection.ConnectInterceptor.intercept(ConnectInterceptor.java:42)\\r\\n\\tat okhttp3.internal.http.RealInterceptorChain.proceed(RealInterceptorChain.java:147)\\r\\n\\tat okhttp3.internal.http.RealInterceptorChain.proceed(RealInterceptorChain.java:121)\\r\\n\\tat okhttp3.internal.cache.CacheInterceptor.intercept(CacheInterceptor.java:93)\\r\\n\\tat okhttp3.internal.http.RealInterceptorChain.proceed(RealInterceptorChain.java:147)\\r\\n\\tat okhttp3.internal.http.RealInterceptorChain.proceed(RealInterceptorChain.java:121)\\r\\n\\tat okhttp3.internal.http.BridgeInterceptor.intercept(BridgeInterceptor.java:93)\\r\\n\\tat okhttp3.internal.http.RealInterceptorChain.proceed(RealInterceptorChain.java:147)\\r\\n\\tat okhttp3.internal.http.RetryAndFollowUpInterceptor.intercept(RetryAndFollowUpInterceptor.java:126)\\r\\n\\tat okhttp3.internal.http.RealInterceptorChain.proceed(RealInterceptorChain.java:147)\\r\\n\\tat okhttp3.internal.http.RealInterceptorChain.proceed(RealInterceptorChain.java:121)\\r\\n\\tat okhttp3.logging.HttpLoggingInterceptor.intercept(HttpLoggingInterceptor.java:213)\\r\\n\\tat okhttp3.internal.http.RealInterceptorChain.proceed(RealInterceptorChain.java:147)\\r\\n\\tat okhttp3.internal.http.RealInterceptorChain.proceed(RealInterceptorChain.java:121)\\r\\n\\tat com.xh.xhcore.common.http.strategy.okhttp.interceptors.RedirectInterceptor.intercept(RedirectInterceptor.kt:13)\\r\\n\\tat okhttp3.internal.http.RealInterceptorChain.proceed(RealInterceptorChain.java:147)\\r\\n\\tat okhttp3.internal.http.RealInterceptorChain.proceed(RealInterceptorChain.java:121)\\r\\n\\tat okhttp3.RealCall.getResponseWithInterceptorChain(RealCall.java:200)\\r\\n\\tat okhttp3.RealCall$AsyncCall.execute(RealCall.java:147)\\r\\n\\tat okhttp3.internal.NamedRunnable.run(NamedRunnable.java:32)\\r\\n\\tat java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1113)\\r\\n\\tat java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:588)\\r\\n\\tat java.lang.Thread.run(Thread.java:818)\\r\\nappend error message: java.lang.Throwable\\r\\n\\tat com.xh.xhcore.common.http.strategy.xh.download.XHDownloadOkHttpProxy$XHDownloadCallbackInC$1.run(XHDownloadOkHttpProxy.java:229)\\r\\n\\tat android.os.Handler.handleCallback(Handler.java:739)\\r\\n\\tat android.os.Handler.dispatchMessage(Handler.java:95)\\r\\n\\tat com.xh.logutils.FireLooper.run(FireLooper.java:76)\\r\\n\\tat android.os.Handler.handleCallback(Handler.java:739)\\r\\n\\tat android.os.Handler.dispatchMessage(Handler.java:95)\\r\\n\\tat android.os.Looper.loop(Looper.java:148)\\r\\n\\tat android.app.ActivityThread.main(ActivityThread.java:7325)\\r\\n\\tat java.lang.reflect.Method.invoke(Native Method)\\r\\n\\tat com.android.internal.os.ZygoteInit$MethodAndArgsCaller.run(ZygoteInit.java:1321)\\r\\n\\tat com.android.internal.os.ZygoteInit.main(ZygoteInit.java:1211)\\r\",\"userId\":\"119245\",\"deviceId\":\"R22K800G00V\",\"versionCode\":\"174\\r\",\"tags\":[\"multiline\"],\"path\":\"/opt/xuehaiserver/log/crash.log\",\"totalMemory\":\"1891MB\\r\",\"@timestamp\":\"2019-10-25T08:34:45.764Z\",\"availableMemory\":\"1019MB\\r\",\"schoolId\":\"4958\\r\",\"packageName\":\"com.xh.arespunc\\r\"}";
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, assistStr);
        producer.send(record);

        producer.close();
    }

    public static void kafka_producer(){
        Properties props = new Properties();

        props.put("bootstrap.servers", brokerList);
        props.put("acks", "1");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        victory.add(0, false);
        victory.add(1, true);

        for(int i=0; i<1; i++){
            JSONObject json = JSONObject.parseObject("{}");
            json.put("season", 5);

            System.out.println(json.toJSONString());

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, 2, "key", json.toJSONString());
            producer.send(record);
        }

        producer.close();
    }

}
