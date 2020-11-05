package com.yuepengfei.java;

import lombok.SneakyThrows;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.LongAdder;

public class LongAdderDemo {

    public static void main(String[] args) throws BrokenBarrierException, InterruptedException {

        LongAdder longAdder = new LongAdder();

        CountDownLatch countDownLatch = new CountDownLatch(100);

        CyclicBarrier cyclicBarrier = new CyclicBarrier(100, new Runnable() {
            @Override
            public void run() {
                System.out.println("所有线程开已准备就绪");
            }
        });

        Semaphore semaphore = new Semaphore(10);


        for (int i = 0; i < 100; i++) {
            new Thread(new Runnable() {
                @SneakyThrows
                @Override
                public void run() {
                    cyclicBarrier.await();//本线程已到位
                    semaphore.acquire();//或许许可
                    for (int j = 0; j < 100; j++) {
                        longAdder.add(1L);
                    }
                    semaphore.release();//释放许可
                    countDownLatch.countDown();
                }
            }).start();
        }
        countDownLatch.await();
        System.out.println("计算结束");
        System.out.println(longAdder.longValue());
    }

}
