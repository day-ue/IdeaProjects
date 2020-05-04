package com.yuepengfei.monitor.groovy;

import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyObject;
import groovy.lang.GroovyShell;
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
    public void classLoaderString() throws Exception{
        GroovyClassLoader loader = new GroovyClassLoader();
        Class aClass = loader.parseClass("def plus(int a, int b){\n" +
                "        return a+b\n" +
                "    }");
        GroovyObject groovyObject = (GroovyObject) aClass.newInstance();
        //这里不能用int数组，具体为啥我也不知道
        Integer[] param = {1, 2};
        Integer result = (Integer)groovyObject.invokeMethod("plus", param);
        System.out.println(result);
    }


    @Test
    public void scriptEngine() throws Exception{
        GroovyScriptEngine engine = new GroovyScriptEngine("./src/main/java/com/yuepengfei/monitor/groovy/");
        Class aClass = engine.loadScriptByName("GroovyApp.groovy");
        GroovyObject groovyObject = (GroovyObject) aClass.newInstance();
        groovyObject.invokeMethod("hello", "你最美丽");
    }

    @Test
    public void  shellGroovy(){
        GroovyShell groovyShell = new GroovyShell();
        groovyShell.evaluate("println(\"he\")");

    }

}
