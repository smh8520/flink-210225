package com.atguigu.utils;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * @author smh
 * @create 2021-08-05 11:27
 */
@Target(FIELD)
@Retention(RUNTIME)
public @interface ClickHouseSinkAnnotation {
}
