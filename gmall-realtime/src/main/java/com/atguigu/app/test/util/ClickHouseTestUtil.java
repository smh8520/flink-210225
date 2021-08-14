package com.atguigu.app.test.util;

import com.atguigu.common.GmallConfig;
import com.atguigu.utils.ClickHouseSinkAnnotation;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author smh
 * @create 2021-08-05 21:17
 */
public class ClickHouseTestUtil {

    public static <T> SinkFunction<T> getSink(String sql){

        JdbcSink.sink(sql, new JdbcStatementBuilder<T>() {
            @Override
            public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                //通过反射获取字段
                Field[] declaredFields = t.getClass().getDeclaredFields();
                //遍历字段
                int offset=0;
                for (int i = 0; i < declaredFields.length; i++) {
                    Field declaredField = declaredFields[i];
                    //设置访问权限
                    declaredField.setAccessible(true);
                    //获取注解
                    ClickHouseSinkAnnotation annotation = declaredField.getAnnotation(ClickHouseSinkAnnotation.class);
                    if(annotation==null){
                        //没有注解标记,则此字段是需要的
                        try {
                            preparedStatement.setObject(i+1-offset,declaredField.get(t));
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }
                    }else{
                        //否则,跳过此字段
                        offset++;
                    }
                }
            }
        },new JdbcExecutionOptions.Builder().withBatchSize(5).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .build());

        return null;
    }
}
