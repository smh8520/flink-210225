package com.atguigu.utils;

import com.atguigu.common.GmallConfig;
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
 * @create 2021-08-05 11:09
 */
public class ClickHouseUtil {

    public static <T> SinkFunction<T> getSink(String sql) {


        return JdbcSink.sink(sql, new JdbcStatementBuilder<T>() {
            @Override
            public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                    //获取字段
                Field[] declaredFields = t.getClass().getDeclaredFields();

                int offset=0;
                //遍历字段
                for (int i = 0; i < declaredFields.length; i++) {
                    Field field = declaredFields[i];
                    ClickHouseSinkAnnotation annotation = field.getAnnotation(ClickHouseSinkAnnotation.class);

                    if(annotation==null){
                        //设置属性权限为可访问的
                        field.setAccessible(true);
                        try {
                            Object o = field.get(t);
                            preparedStatement.setObject(i+1-offset, o);
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }
                    }else{
                        offset++;
                    }
                }
            }
        }, JdbcExecutionOptions.builder().withBatchSize(5).build(),
               new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                       .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                       .withUrl(GmallConfig.CLICKHOUSE_URL).build());

    }

}
