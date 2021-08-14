package com.atguigu.app.testt.testUtils;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author smh
 * @create 2021-08-01 19:33
 */
public class MyJDBCUtils {
    public static <T> List<T> getInformation(Connection conn,String sql,Class<T> clazz,boolean toCamel){
        PreparedStatement preparedStatement=null;
        ArrayList<T> list = new ArrayList<>();
        //查询数据
        try {
            preparedStatement = conn.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();
            //获取元数据信息,获取一共几列
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            //遍历结果集
            while(resultSet.next()){
                //创建泛型对象
                T t = clazz.newInstance();
                //遍历每一列的列名,值
                for (int i = 1; i <=columnCount ; i++) {
                    String columnName = metaData.getColumnName(i);
                    //根据参数将大写下划线形式的列名改为小驼峰形式
                    if(toCamel){
                        columnName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }
                    Object value = resultSet.getObject(i);
                    //利用工具包将每一列放入泛型对象中
                    BeanUtils.setProperty(t,columnName,value);
                }
                //将对象添加到集合中
                list.add(t);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(preparedStatement!=null){
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        //返回结果集
        return list;
    }
}
