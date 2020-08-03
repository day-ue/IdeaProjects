package com.yuepengfei.java;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class AwaitSignalDemo {
    static ReentrantLock lock = new ReentrantLock();
    static Condition condition = lock.newCondition();

    public static void main(String[] args) throws InterruptedException {

        Thread t = new Thread(() -> {
            lock.lock();
            try {
                condition.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }finally {
                lock.unlock();
            }
            System.out.println("线程" + Thread.currentThread().getName());
        });
        t.setName("线程2");
        t.start();

        Thread.sleep(1000);
        new Thread(() -> {
            lock.lock();
            try {
                condition.signal();
            } finally {
                lock.unlock();
            }
        }).start();
    }
}
