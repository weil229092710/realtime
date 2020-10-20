import redis.clients.jedis.Jedis;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class RedisStringJava {
    public static void main(String[] args) {
        //连接本地的 Redis 服务
        Jedis jedis = new Jedis("localhost",6379);
        //jedis.auth("xhcodis@321");
        System.out.println("连接成功");
        //设置 redis 字符串数据
        jedis.set("runoobkey", "www.runoob.com");
        // 获取存储的数据并输出

        Set<String> keys = jedis.keys("1:??:36368");
        for (String key : keys) {
            List list=new ArrayList<Integer>();
                list.add(key.split(":")[1]);



        }
    }
}