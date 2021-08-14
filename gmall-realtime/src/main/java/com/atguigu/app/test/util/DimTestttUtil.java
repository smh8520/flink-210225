package com.atguigu.app.test.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.RedisUtil;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.List;

/**
 * @author smh
 * @create 2021-08-01 23:12
 */
public class DimTestttUtil {
    public static JSONObject getInfo(Connection conn,String tableName,String id){
        Jedis jedis = RedisUtil.getJedis();
        String key = "DIM:"+tableName+":"+id;
        String s = jedis.get(key);
        if(s!=null){
            JSONObject jsonObject = JSONObject.parseObject(s);
            jedis.set(key,jsonObject.toJSONString());
            jedis.expire(key,24*60*60);
            jedis.close();
        }
        String sql = "select * from "+ GmallConfig.HBASE_SCHEMA+"."+tableName
                +" where id='"+id+"'";

        List<JSONObject> list = JDBCTestttUtil.getList(conn, sql, JSONObject.class, false);
        JSONObject jsonObject = list.get(0);
        jedis.set(key,jsonObject.toJSONString());
        jedis.expire(key,24*60*60);
        return jsonObject;
    }

    public static void deleteKey(String key){
        Jedis jedis = RedisUtil.getJedis();
        jedis.del(key);
        jedis.close();
    }
}
