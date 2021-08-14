package com.atguigu.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import redis.clients.jedis.Jedis;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
/**
 * @author smh
 * @create 2021-08-01 15:41
 */
/*
我们需要从hbase中查询维度数据.如果直接查询的话,第一次需要先去zookeeper中获取元数据位置,然后再去
元数据位置获取元数据,再从元数据中获取数据所在位置,再去读.
之后虽然会缓存元数据信息,少了两次交互,但是每次读还是需要去和hbase具体存储数据的节点交互,效率有点低,数据量过大的时候会有延迟.
我们可以使用redis来缓存查过的数据,这样,对于一些查过的数据直接去redis中找
如果redis中找不到再去hbase中找,找到之后再把数据存到redis中.
但是如果一直存redis,会使内存数据越来越多.所以可以给redis中存的数据设置一个超时时间.
如果在超时时间之内数据被再次读取,则重置超时时间.
 */
public class DimUtils {
    public static JSONObject getDimInfo(Connection conn,String tableName,String id){
        //先去redis中查询数据
        //获取redis连接
        Jedis jedis = RedisUtil.getJedis();
        //编写key
        String key = "DIM:"+tableName+":"+id;
        String value = jedis.get(key);
        //如果数据不为空
        if(value!=null){
            JSONObject result = JSONObject.parseObject(value);
            //重置生命周期
            jedis.expire(key,24*60*60);
            //关闭jedis连接
            jedis.close();
            return result;
        }
        //否则,从hbase中查询数据
        //编写sql
        String sql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id='" + id + "'";
        //执行查询
        List<JSONObject> jsonObjects = JDBCUtils.queryList(conn, sql, JSONObject.class, false);
        JSONObject result = jsonObjects.get(0);
        //将查询到的结果存到redis中
        jedis.set(key,result.toJSONString());
        //设置过期时间
        jedis.expire(key,24*60*60);
        jedis.close();
        return result;
    }
    /*如果维度数据发生改变,那么phoniex中的数据也会发生对应的改变,但是此时redis中存储的数据并没有改.
    如果这条数据还一直被查询,过期时间就会一直刷新,导致读出的是错误数据.
    所以当维度数据发生更新的时候,要删除redis中对应key的数据.*/
    public static void deleteRedisKey(String redisKey){
        Jedis jedis = RedisUtil.getJedis();
        jedis.del(redisKey);
        jedis.close();
    }
    public static void main(String[] args) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        System.out.println(getDimInfo(conn, "DIM_USER_INFO", "960"));
    }
}
