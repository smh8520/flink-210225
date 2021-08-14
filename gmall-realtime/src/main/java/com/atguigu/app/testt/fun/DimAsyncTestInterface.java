package com.atguigu.app.testt.fun;

import com.alibaba.fastjson.JSONObject;

/**
 * @author smh
 * @create 2021-08-01 20:03
 */
public interface DimAsyncTestInterface<T> {
     void merge(T input, JSONObject dimInfo);

      String getId(T input);
}
