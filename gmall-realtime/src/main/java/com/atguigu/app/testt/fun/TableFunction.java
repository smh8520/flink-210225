package com.atguigu.app.testt.fun;

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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author smh
 * @create 2021-07-31 17:16
 */
public class TableFunction extends BroadcastProcessFunction<JSONObject,String,JSONObject> {

    private MapStateDescriptor<String, TableProcess> mapState;
    private Connection conn;

    public TableFunction() {
    }

    public TableFunction(MapStateDescriptor<String, TableProcess> mapState) {
        this.mapState = mapState;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //获取phoniex连接
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        //解析参数
        JSONObject jsonObject = JSONObject.parseObject(value);

        TableProcess tableProcess = JSONObject.parseObject(jsonObject.getString("data"), TableProcess.class);

        //判断是否需要建表,如果配置文件是刚插入的,而且是发往hbase的 则需要建表
        if("insert".equals(jsonObject.getString("type")) && TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())){
            createTableTest(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend());
        }
        //封装为状态
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapState);
        String key = tableProcess.getSourceTable() + "_" + tableProcess.getOperateType();
        broadcastState.put(key,tableProcess);
    }

    private void createTableTest(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        //完善信息
        if(sinkPk == null){
            sinkPk="id";
        }
        if (sinkExtend == null) {
            sinkExtend="";
        }
        try {
        //编写建表语句
        StringBuilder sql = new StringBuilder();
        sql.append("create table if not exists ")
                .append(GmallConfig.HBASE_SCHEMA)
                .append(".")
                .append(sinkTable)
                .append("(");
        String[] columns = sinkColumns.split(",");
        for (int i = 0; i < columns.length; i++) {
            String column = columns[i];
            if(sinkPk.equals(column)){
                sql.append(column).append(" varchar primary key");
            }else{
                sql.append(column).append(" varchar");
            }

            if(i<columns.length-1){
                sql.append(",");
            }
        }
        sql.append(")").append(sinkExtend);

        System.out.println(sql);

        //预编译sql语句,执行,提交
            PreparedStatement preparedStatement = conn.prepareStatement(sql.toString());
            preparedStatement.execute();
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("建表" + sinkTable + "失败！");
        }
    }

    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //解析参数,获取配置信息
        String key = value.getString("tableName")+"_"+value.getString("type");
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapState);
        TableProcess tableProcess = broadcastState.get(key);

       if(tableProcess!=null){
           //过滤字段
           filterFields(value.getJSONObject("data"),tableProcess.getSinkColumns());
           //将sinkTable添加到value中便于后面操作
           String sinkTable = tableProcess.getSinkTable();
           value.put("sinkTable",sinkTable);

           String sinkType = tableProcess.getSinkType();
           if(TableProcess.SINK_TYPE_HBASE.equals(sinkType)){
               ctx.output(new OutputTag<JSONObject>("hbase"){},value);
           }else if(TableProcess.SINK_TYPE_KAFKA.equals(sinkType)){
               out.collect(value);
           }

       }else{
           System.out.println(key+"不存在!");
       }

    }

    private void filterFields(JSONObject data, String sinkColumns) {
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        List<String> columnList = Arrays.asList(sinkColumns.split(","));

        entries.removeIf(x->!columnList.contains(x.getKey()));
    }


}
