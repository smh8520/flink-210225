package com.atguigu.app.testt.fun;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.testt.testUtils.DimInfoTestUtil;
import com.atguigu.app.testt.testUtils.ThreadPoolTestUtil;
import com.atguigu.common.GmallConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author smh
 * @create 2021-08-01 19:58
 */
public abstract class DimAsyncTestFunction<T> extends RichAsyncFunction<T,T> implements DimAsyncTestInterface<T> {
    private Connection connection;
    private ThreadPoolExecutor pool;
    private String tableName;
    public DimAsyncTestFunction(String tableName){
        this.tableName=tableName;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        pool= ThreadPoolTestUtil.getInstance();
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection= DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        //获取id,表名
        String id = getId(input);
        //查询数据
        JSONObject dimInfo = DimInfoTestUtil.getDimInfo(connection, tableName, id);
        //合并数据
        merge(input,dimInfo);
        //输出结果
        resultFuture.complete(Collections.singleton(input));
    }



    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("TimeOut!");
    }
}
