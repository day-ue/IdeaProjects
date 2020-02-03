package com.yuepengfei.monitor.groovy;

import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyObject;
import groovy.util.GroovyScriptEngine;
import org.junit.Test;


import java.io.File;

/**
 * 风控中较为常用，作为规则引擎，实现动态更新代码
 * 结合flink ECP使用
 */

public class Main {

    @Test
    public void classLoader() throws Exception{
        GroovyClassLoader loader = new GroovyClassLoader();
        File file = new File("./src/main/java/com/yuepengfei/monitor/groovy/GroovyApp.groovy");
        //这里也可以直接将文件中的内容以一个字符串的形式传入
        Class aClass = loader.parseClass(file);
        GroovyObject groovyObject = (GroovyObject) aClass.newInstance();
        groovyObject.invokeMethod("hello", "你最美丽");
    }

    @Test
    public void scriptEngine() throws Exception{
        GroovyScriptEngine engine = new GroovyScriptEngine("./src/main/java/com/yuepengfei/monitor/groovy/");
        Class aClass = engine.loadScriptByName("GroovyApp.groovy");
        GroovyObject groovyObject = (GroovyObject) aClass.newInstance();
        groovyObject.invokeMethod("hello", "你最美丽");
    }


}
