package com.atguigu.app.testt.fun;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.testt.testUtils.DimInfoTestUtil;
import com.atguigu.common.GmallConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * @author smh
 * @create 2021-07-31 17:42
 */
public class MySinkFunction extends RichSinkFunction<JSONObject> {
    private Connection conn;
    @Override
    public void open(Configuration parameters) throws Exception {
        //获取phoniex连接
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement=null;
        try {
            //编写sql语句
            String sql=getUpSertSql(value);

            System.out.println(sql);

         preparedStatement = conn.prepareStatement(sql);
         //获取redisKey
            String key = "Dim:"+value.getString("sinkTable").toUpperCase()
                    +":"+value.getJSONObject("data").getString("id");
            //删除redis中的缓存
            DimInfoTestUtil.deleteRedisKey(key);

         preparedStatement.execute();
         conn.commit();
        } catch (SQLException e) {
            System.out.println("插入数据"+value.getJSONObject("data")+"出错!");
        } finally {
            if(preparedStatement!=null){
                preparedStatement.close();
            }
        }
    }

    private String getUpSertSql(JSONObject value) {
        JSONObject data = value.getJSONObject("data");
        String sinkTable = value.getString("sinkTable");

        Set<String> key = data.keySet();
        Collection<Object> values = data.values();

        String s = "upsert into " + GmallConfig.HBASE_SCHEMA
                + "." + value.getString(sinkTable) + "("
                + StringUtils.join(key, ",")
                + "values('"
                + StringUtils.join(values, "','")
                + "')";
        return s;
    }
}
