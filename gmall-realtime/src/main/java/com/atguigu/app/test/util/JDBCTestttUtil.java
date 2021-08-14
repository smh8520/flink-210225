package com.atguigu.app.test.util;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author smh
 * @create 2021-08-01 23:08
 */
public class JDBCTestttUtil {
    public static <T> List<T> getList(Connection conn,String sql,Class<T> clazz,boolean toCamel){
        PreparedStatement preparedStatement=null;
        ArrayList<T> list = new ArrayList<>();
        try {
             preparedStatement = conn.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (resultSet.next()){
                T t = clazz.newInstance();
                for (int i = 1; i <=columnCount ; i++) {
                    String columnName = metaData.getColumnName(i);
                    if(toCamel){
                        columnName= CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName);
                    }
                    Object v = resultSet.getObject(i);
                    BeanUtils.setProperty(t,columnName,v);
                }
                list.add(t);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }
}
