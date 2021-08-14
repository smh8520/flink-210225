package com.atguigu.app.testt.testUtils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.RedisUtil;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.List;

/**
 * @author smh
 * @create 2021-08-01 19:40
 */
//查询具体数据,并使用redis进行优化
public class DimInfoTestUtil {
    public static JSONObject getDimInfo(Connection conn,String tableName,String id){
        //创建redis连接,定义key,查询缓存中是否存在数据
        Jedis jedis = RedisUtil.getJedis();
        String key="Dim:"+tableName+":"+id;
        String s = jedis.get(key);
        if(s!=null){
            JSONObject jsonObject = JSONObject.parseObject(s);
            //重置超时时间
            jedis.expire(key,24*60*60);
            //关闭连接,返回对象
            jedis.close();
            return jsonObject;
        }
        //如果redis中不存在对应的信息,则需要去hbase中查询数据
        //编写sql
        String sql = "select * from "+ GmallConfig.HBASE_SCHEMA+"."+tableName
                +" where id='"+id+"'";
        //查询
        List<JSONObject> information = MyJDBCUtils.getInformation(conn, sql, JSONObject.class, false);
        //取值
        JSONObject result = information.get(0);
        //将数据存到redis缓存中,并设置生命周期
        jedis.set(key,result.toJSONString());
        jedis.expire(key,24*60*60);
        //关闭连接
        jedis.close();
        //返回结果
        return result;
    }

    /*
    如果维度信息改变,phoenix中数据也会实时变化,但是redis缓存不会变化,而且如果这个缓存一直被查询,就会一直不消失.
    为了同步缓存与hbase中的数据,在hbase中插入数据的时候要删除缓存中对应的数据
     */
    public static void deleteRedisKey(String key){
        Jedis jedis = RedisUtil.getJedis();
        jedis.del(key);
        //关闭连接
        jedis.close();
    }
}
