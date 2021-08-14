package com.atguigu.app.function;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * @author smh
 * @create 2021-08-06 18:16
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class MyUDTF extends TableFunction<Row> {
    public void eval(String str) {
        List<String> strings = IKSplitFunction.splitWord(str);
        for (String string : strings) {
            collect(Row.of(string));
        }
    }
}
