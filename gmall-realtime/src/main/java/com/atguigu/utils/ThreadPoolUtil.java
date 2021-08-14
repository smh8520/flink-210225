package com.atguigu.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author smh
 * @create 2021-08-01 18:50
 */
public class ThreadPoolUtil {
    public static ThreadPoolExecutor pool;
    //私有化构造器
    private ThreadPoolUtil(){

    }

    public static ThreadPoolExecutor getInstance(){
        if(pool==null){
            synchronized (ThreadPoolUtil.class){
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
