package com.yuepengfei.flink;

import com.yuepengfei.bean.LoginEvent;
import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyObject;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CEPGroovy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        ArrayList<LoginEvent> list = new ArrayList<>();
        list.add(new LoginEvent("1", "192.168.0.1", "fail"));
        list.add(new LoginEvent("1", "192.168.0.2", "fail"));
        list.add(new LoginEvent("1", "192.168.0.3", "fail"));
        list.add(new LoginEvent("2", "192.168.10,10", "success"));

        DataStreamSource<LoginEvent> loginEventDataStreamSource = env.fromCollection(list);

        /**
         * 在流里面加一个flag，当flag为1时重新加载规则
         */
        GroovyClassLoader loader = new GroovyClassLoader();
        File file = new File("./src/main/java/com/yuepengfei/script/Rule.groovy");
        Class aClass = loader.parseClass(file);
        GroovyObject groovyObject = (GroovyObject) aClass.newInstance();
        Pattern<LoginEvent, LoginEvent> pattern = (Pattern<LoginEvent, LoginEvent>) groovyObject.invokeMethod("run", null);

        PatternStream<LoginEvent> patternStream = CEP.pattern(loginEventDataStreamSource, pattern);
        

        patternStream.select(new PatternSelectFunction<LoginEvent, LoginEvent>() {
            @Override
            public LoginEvent select(Map<String, List<LoginEvent>> map) throws Exception {
                List<LoginEvent> next = map.get("next");
                return new LoginEvent(next.get(0).userId,next.get(0).ip,next.get(0).type_type);
            }
        }).print("next");


        PatternStream<LoginEvent> pattern1 = CEP.pattern(loginEventDataStreamSource, pattern);
        pattern1.select(new PatternSelectFunction<LoginEvent, LoginEvent>() {
            @Override
            public LoginEvent select(Map<String, List<LoginEvent>> map) throws Exception {
                List<LoginEvent> start = map.get("begin");
                return new LoginEvent(start.get(0).userId, start.get(0).ip ,start.get(0).type_type);
            }
        }).print("begin");


        env.execute();
    }
}
