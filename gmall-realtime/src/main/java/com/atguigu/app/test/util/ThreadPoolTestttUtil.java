package com.atguigu.app.test.util;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author smh
 * @create 2021-08-01 23:16
 */
public class ThreadPoolTestttUtil {
    private static ThreadPoolExecutor pool;
    private ThreadPoolTestttUtil(){

    }
    public static ThreadPoolExecutor getInstance(){
        if(pool==null){
            synchronized (ThreadPoolExecutor.class){
                if(pool==null){
                    pool=new ThreadPoolExecutor(4,
                            20,
                            60,
                            TimeUnit.SECONDS,
                            new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE));
                }
            }
        }
        return pool;
    }
}
