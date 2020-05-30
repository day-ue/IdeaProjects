package com.yuepengfei.java.WaitNotify;

public class WaitNotity {

    static Object lock = new Object();
    static volatile boolean flag = true;

    public static void main(String[] args) throws InterruptedException {

        new Thread(() ->{
            //todo wait,notify必须在锁代码块中，不然会报错
            synchronized(lock) {
                while(flag) {
                    try {
                        lock.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                // 干活
                System.out.println("干活了");
            }
        }).start();

        //另一个线程
        new Thread(()->{
            synchronized(lock) {
                flag = false;
                lock.notifyAll();
            }
        }).start();

    }
}
