package com.atguigu.app.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DimUtils;
import com.atguigu.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author smh
 * @create 2021-08-01 18:53
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimAsyncInterface<T> {
    private Connection connection;
    private ThreadPoolExecutor pool;
    private String tableName;
    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        pool= ThreadPoolUtil.getInstance();
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection=DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }
    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        pool.submit(new Runnable() {
            @Override
            public void run() {
                  //id,表名
                String id=getId(input);
                //查询数据
                JSONObject dimInfo = DimUtils.getDimInfo(connection, tableName, id);
                //合并数据
                if(dimInfo!=null){
                    mergeInfo(input,dimInfo);
                }

                //写出数据
                resultFuture.complete(Collections.singletonList(input));
            }
        });
    }
    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("TimeOut!"+input);
    }
}
