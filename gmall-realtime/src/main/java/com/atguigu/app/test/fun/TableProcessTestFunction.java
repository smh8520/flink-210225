package com.atguigu.app.test.fun;

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
 * @create 2021-07-29 18:53
 */
public class TableProcessTestFunction extends BroadcastProcessFunction<JSONObject, String,JSONObject> {
    private MapStateDescriptor<String, TableProcess> stringTableProcessMapStateDescriptor;
    //连接对象
    private Connection conn;
    public TableProcessTestFunction(){

    }

    public TableProcessTestFunction(MapStateDescriptor<String, TableProcess> x){
        this.stringTableProcessMapStateDescriptor=x;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
            //解析广播数据,将广播数据转为json字符串
        JSONObject jsonObject = JSONObject.parseObject(value);
        TableProcess tableProcess = JSONObject.parseObject(jsonObject.getString("data"), TableProcess.class);
        //创建表
        //如果配置信息是新插入的,且是往hbase中发数据,则创建表
        if("insert".equals(jsonObject.getString("type")) && TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())){
            createTable(tableProcess.getSinkPk(),
                        tableProcess.getSinkExtend(),
                    tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns());
        }
        //保存为状态
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(stringTableProcessMapStateDescriptor);
        //以原表名和数据的操作类型作为key,方便按照新增或改变来将数据输往不同的目的地
        String key = tableProcess.getSourceTable()+"_"+tableProcess.getOperateType();
        broadcastState.put(key,tableProcess);
    }

    private void createTable(String sinkPk, String sinkExtend, String sinkTable, String sinkColumns) {
        //先将类型转换,如果extend为null,则变为空串.主键没有指定的话以id作为主键
        if(sinkExtend == null){
            sinkExtend="";
        }
        if(sinkPk==null){
            sinkPk="id";
        }
        System.out.println(sinkPk);
        try {
        //编写建表语句
        StringBuilder createTableSql = new StringBuilder();
        createTableSql.append("create table if not exists ")//如果表不存在则创建表
                .append(GmallConfig.HBASE_SCHEMA)//命名空间
                .append(".")
                .append(sinkTable)//表名
                .append("(");

        //将字段切分,挨个添加到建表语句中,并根据主键进行判断
        String[] columns = sinkColumns.split(",");
        for (int i = 0; i < columns.length; i++) {
            String column = columns[i];
            //如果该字段是主键
            if(sinkPk.equals(column)){
                createTableSql.append(column).append(" varchar primary key");
            }else{
                //否则
                createTableSql.append(column).append(" varchar");
            }

            //判断是不是最后一个字段,不是的话加","
            if(i< columns.length-1){
                createTableSql.append(",");
            }
        }
        //补上后面的括号和其他信息
        createTableSql.append(")").append(sinkExtend);

        //输出建表语句
            System.out.println(createTableSql);
        //预编译sql语句
            PreparedStatement preparedStatement = conn.prepareStatement(createTableSql.toString());
        //执行
            preparedStatement.execute();
            //提交
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            //如果建表失败,则停止程序
            throw new RuntimeException(sinkTable+"表创建失败!");
        }

    }

    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(stringTableProcessMapStateDescriptor);
        String key = value.getString("tableName") + "_" + value.getString("type");
        //读取广播状态
        TableProcess tableProcess = broadcastState.get(key);

        //输出对应的策略
        System.out.println(tableProcess);

        //tableProcess是有可能等于null的,因为如果OperateType只写了insert的话,update的数据是查不到的.
        if(tableProcess != null){
            //过滤数据,只往目的地发送我们需要的字段
            filterFields(value.getJSONObject("data"),tableProcess.getSinkColumns());

            String sinkType = tableProcess.getSinkType();
            //将目的表加入到value中,便于之后的操作
            value.put("sinkTable",tableProcess.getSinkTable());
            if(TableProcess.SINK_TYPE_KAFKA.equals(sinkType)){
                out.collect(value);
            }else if(TableProcess.SINK_TYPE_HBASE.equals(sinkType)){
                ctx.output(new OutputTag<JSONObject>("hbase"){},value);
            }else{
                System.out.println("目的地不存在");
            }
        }else{
            System.out.println(key+"不存在");
        }
    }

    private void filterFields(JSONObject data, String sinkColumns) {
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        //切分sinkColumns,并将它转化为集合(便于使用contains方法)
        List<String> columns = Arrays.asList(sinkColumns.split(","));
        //如果字段不包含在想要的字段列表中,就删除
        entries.removeIf(value->!columns.contains(value.getKey()));
    }


}
