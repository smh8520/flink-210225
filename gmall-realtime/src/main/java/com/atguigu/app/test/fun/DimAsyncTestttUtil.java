package com.atguigu.app.test.fun;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.test.util.DimTestttUtil;
import com.atguigu.app.test.util.ThreadPoolTestttUtil;
import com.atguigu.common.GmallConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author smh
 * @create 2021-08-01 23:18
 */
public abstract class DimAsyncTestttUtil<T> extends RichAsyncFunction<T,T>implements DimAsyncTestttInterface<T> {
    private Connection conn;
    private ThreadPoolExecutor pool;
    private String tableName;

    public DimAsyncTestttUtil(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        pool= ThreadPoolTestttUtil.getInstance();
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        conn= DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {

        String id=getId(input);
        JSONObject info = DimTestttUtil.getInfo(conn, tableName, id);
        merge(input,info);

    }



    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("TimeOut!");
    }
}
