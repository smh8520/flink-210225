package com.atguigu;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/**
 * @author smh
 * @create 2021-07-27 20:05
 */
public class FlinkSQLCDC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//        tableEnv.getConfig().setIdleStateRetention();

        tableEnv.executeSql("CREATE TABLE base_trademark (\n" +
                " id INT,\n" +
                " tm_name String,\n" +
                " logo_url String\n" +
                ") WITH (\n" +
                " 'connector' = 'mysql-cdc',\n" +
                " 'hostname' = 'hadoop102',\n" +
                " 'port' = '3306',\n" +
                " 'username' = 'root',\n" +
                " 'password' = '123456',\n" +
                " 'database-name' = 'gmall_flink',\n" +
                " 'table-name' = 'base_trademark'\n" +
                ")");
            tableEnv.executeSql("select * from base_trademark").print();


        env.execute();

    }
}
