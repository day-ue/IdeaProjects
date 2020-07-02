package com.yuepengfei.java;

public class InitClassDemo {

    String a = "a";
    static String b = "b";


    {
        System.out.println("普通代码块");
        System.out.println("普通代码块"+ a);
        System.out.println("普通代码块"+ b);
    }

    static {
        System.out.println("静态代码块");
        System.out.println("静态代码块"+ b);
    }



    public InitClassDemo() {
        System.out.println("构造函数");
        System.out.println(a);
        System.out.println(b);
    }

    public static void main(String[] args) {
        new InitClassDemo();
    }
}
