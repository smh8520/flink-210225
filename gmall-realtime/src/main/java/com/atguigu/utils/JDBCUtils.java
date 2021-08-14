package com.atguigu.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import com.google.common.base.CaseFormat;
/**
 * @author smh
 * @create 2021-08-01 15:13
 */
public class JDBCUtils {

    public static <T> List<T> queryList(Connection conn,String sql,Class<T> clazz,boolean toCamel){
        PreparedStatement preparedStatement=null;
        List<T> list = new ArrayList<>();
        try {
            //预编译
            preparedStatement = conn.prepareStatement(sql);
            //执行sql返回查询结果
            ResultSet resultSet = preparedStatement.executeQuery();
            //获取查询结果的元数据
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            //遍历查询结果
            while(resultSet.next()){
                T t = clazz.newInstance();
                //获取数据的key和value,封装成jsonObject
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    //名字格式是否要从下划线转为小驼峰
                    if(toCamel){
                        columnName=CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName);
                    }
                    Object value = resultSet.getObject(i);
                    BeanUtils.setProperty(t,columnName,value);
                }
                //将数据装入集合中
                list.add(t);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //关闭连接
            if(preparedStatement!=null){
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return list;
    }

    public static void main(String[] args) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        //编写sql语句
        String sql = "select * from GMALL2021_REALTIME.DIM_USER_INFO where id = '960'";
        System.out.println(queryList(conn, sql, JSONObject.class, true));
    }
}
