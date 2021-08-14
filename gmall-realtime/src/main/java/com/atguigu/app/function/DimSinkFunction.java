package com.atguigu.app.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DimUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * @author smh
 * @create 2021-07-29 18:11
 */
public class    DimSinkFunction extends RichSinkFunction<JSONObject> {

    //声明Phoenix连接
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement=null;
        //编写sql语句
        try {
            String upsertSQL=getUpSertSql(value.getString("sinkTable"),value.getJSONObject("data"));
            System.out.println(upsertSQL);

           preparedStatement = connection.prepareStatement(upsertSQL);
            //执行sql语句
            //如果数据更新,删除redis中的对应缓存
            String redisKey="DIM:"+value.getString("sinkTable").toUpperCase()
                    +":"+value.getJSONObject("data").getString("id");
            DimUtils.deleteRedisKey(redisKey);
            preparedStatement.execute();
            connection.commit();
        } catch (SQLException e) {
            System.out.println("插入数据"+value.getJSONObject("data")+"出错!");
        } finally {
            if(preparedStatement !=null){
                preparedStatement.close();
            }
        }

    }

    private String getUpSertSql(String sinkTable, JSONObject data) {
        Set<String> keys = data.keySet();
        Collection<Object> values = data.values();


        return "upsert into "+GmallConfig.HBASE_SCHEMA+"."+sinkTable+"("
                + StringUtils.join(keys,",")+")"
                +"values('"
                +StringUtils.join(values,"','")+"')";
    }
}
