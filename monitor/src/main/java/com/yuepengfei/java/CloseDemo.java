package com.yuepengfei.java;

public class CloseDemo {


    static {
        System.out.println("初始化资源");
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                close();
            }
        }));
    }



    public static void main(String[] args) {
        System.out.println("主程序");
    }


    public static void close(){
        System.out.println("关闭资源");
    }

}
