package com.atguigu.app.test.fun;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
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
 * @create 2021-07-29 19:41
 */
public class DimHBaseSink extends RichSinkFunction<JSONObject> {
    //创建连接对象
    private Connection conn;
    @Override
    public void open(Configuration parameters) throws Exception {
        //加载驱动
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        //连接对象初始化
         conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        //获取数据
        JSONObject data = value.getJSONObject("data");
        //获取目标表
        String sinkTable = value.getString("sinkTable");

        PreparedStatement preparedStatement=null;
        try {
            //编写sql语句
            String upsertSql=getSql(data,sinkTable);

            System.out.println(upsertSql);

            //预编译sql语句
            preparedStatement= conn.prepareStatement(upsertSql);

            preparedStatement.execute();

            conn.commit();
        } catch (SQLException e) {
            System.out.println(data+"数据插入失败");
        } finally {
            if(preparedStatement != null){
                preparedStatement.close();
            }
        }

    }

    private String getSql(JSONObject data, String sinkTable) {
        Set<String> keys = data.keySet();
        Collection<Object> values = data.values();

        return "upsert into "+GmallConfig.HBASE_SCHEMA+"."+sinkTable+"("
                + StringUtils.join(keys,",")+")"
                +"values('"+StringUtils.join(values,"','")+"')";
    }
}
