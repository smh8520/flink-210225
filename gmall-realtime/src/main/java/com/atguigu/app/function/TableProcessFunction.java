package com.atguigu.app.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;


/**
 * @author smh
 * @create 2021-07-29 13:45
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    private Connection connection;



    //定义属性,状态描述器
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction() {
    }

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //加载驱动
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        //获取连接
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

    }

    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

        //解析配置信息,将配置信息改为json
        JSONObject jsonObject = JSONObject.parseObject(value);
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("data"), TableProcess.class);
        //创建库,表
        //检验表是否存在
        String sinkType = tableProcess.getSinkType();
        String type = jsonObject.getString("type");

        if ("insert".equals(type) && TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
            //创建表
            createTable(tableProcess.getSinkColumns(), tableProcess.getSinkPk(), tableProcess.getSinkExtend()
                    , tableProcess.getSinkTable());
        }
        //保存状态
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "_" + tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);
        System.out.println(key);
    }

    /**
     * 创建表
     *
     * @param sinkColumns 字段
     * @param sinkPk      主键
     * @param sinkExtend  其他
     * @param sinkTable   表名
     */
    private void createTable(String sinkColumns, String sinkPk, String sinkExtend, String sinkTable) {
        //处理字段,因为extend没有的话会为null,要修改为空串
        if (sinkExtend == null) {
            sinkExtend = "";
        }
        //主键默认为id
        if (sinkPk == null) {
            sinkPk = "id";
        }
        try {
            //获取建表语句
            StringBuilder createTableSQL = new StringBuilder();

            createTableSQL.append("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");
            //添加字段信息
            String[] columns = sinkColumns.split(",");
            for (int i = 0; i < columns.length; i++) {
                String column = columns[i];
                if (sinkPk.equals(column)) {
                    createTableSQL.append(column).append(" varchar primary key");
                } else {
                    createTableSQL.append(column).append(" varchar");
                }

                if (i < columns.length - 1) {
                    createTableSQL.append(",");
                }
            }

            createTableSQL.append(")").append(sinkExtend);
            //输出建表语句

            System.out.println(createTableSQL);
            //执行预编译sql

            PreparedStatement preparedStatement = connection.prepareStatement(createTableSQL.toString());
            //执行sql语句并提交
            preparedStatement.execute();
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("建表" + sinkTable + "失败！");
        }
    }

    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //读取广播状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //获取key
        String key = value.getString("tableName") + "_" + value.getString("type");
        TableProcess tableProcess = broadcastState.get(key);
        System.out.println(tableProcess);


        if (tableProcess != null) {
            //过滤字段
            filterColumn(tableProcess.getSinkColumns(), value.getJSONObject("data"));

            //分流
            String sinkType = tableProcess.getSinkType();

            value.put("sinkTable",tableProcess.getSinkTable());
            if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
                ctx.output(new OutputTag<JSONObject>("hbase") {
                }, value);
            } else if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)) {
                out.collect(value);
            }
        }else{
            System.out.println(key+"不存在!");
        }
    }

    private void filterColumn(String sinkColumns, JSONObject data) {
        List<String> columns = Arrays.asList(sinkColumns.split(","));

        Set<Map.Entry<String, Object>> entries = data.entrySet();
//        entries.removeIf(next->!columns.contains(next.getKey()));

        //将多余的字段过滤掉
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> next = iterator.next();
            if (!columns.contains(next.getKey())) {
                iterator.remove();
            }
        }

    }


}
