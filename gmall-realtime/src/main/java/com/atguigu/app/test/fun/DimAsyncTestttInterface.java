package com.atguigu.app.test.fun;

import com.alibaba.fastjson.JSONObject;

/**
 * @author smh
 * @create 2021-08-01 23:21
 */
public interface DimAsyncTestttInterface<T> {
    abstract void merge(T input, JSONObject info);

    abstract String getId(T input);
}
