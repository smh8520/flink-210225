package com.atguigu.app.testt.testUtils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author smh
 * @create 2021-08-01 19:55
 */
public class ThreadPoolTestUtil {
    public static ThreadPoolExecutor pool;
    private ThreadPoolTestUtil(){

    }

    public static ThreadPoolExecutor getInstance(){
        if(pool==null){
            synchronized (ThreadPoolTestUtil.class){
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
