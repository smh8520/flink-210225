package com.atguigu.app.function;

import com.alibaba.fastjson.JSONObject;

/**
 * @author smh
 * @create 2021-08-01 19:02
 */
public interface DimAsyncInterface<T> {
    void mergeInfo(T input, JSONObject dimInfo);

    //获取方法名
    String getId(T input);
}
