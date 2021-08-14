package com.atguigu.app.function;

import com.sun.org.apache.bcel.internal.generic.NEW;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author smh
 * @create 2021-08-06 17:49
 */
public class IKSplitFunction {
    public static List<String> splitWord(String source){
        ArrayList<String> list = new ArrayList<>();

        StringReader stringReader = new StringReader(source);

        IKSegmenter ikSegmenter = new IKSegmenter(stringReader, false);

        try {
            Lexeme next = ikSegmenter.next();
            while (next!=null){
                String lexemeText = next.getLexemeText();
                if(lexemeText!=null && lexemeText.length()>0){
                    list.add(lexemeText);
                }
                next=ikSegmenter.next();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }

    public static void main(String[] args) {
        System.out.println(splitWord(""));
    }
}
