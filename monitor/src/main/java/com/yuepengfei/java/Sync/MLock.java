package com.yuepengfei.java.Sync;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class MLock implements Lock {
    static MSync sync = new MSync();

    @Override
// 尝试，不成功，进入等待队列
    public void lock() {
        sync.acquire(1);
    }
    @Override
// 尝试，不成功，进入等待队列，可打断
    public void lockInterruptibly() throws InterruptedException {
        sync.acquireInterruptibly(1);
    }
    @Override
// 尝试一次，不成功返回，不进入队列
    public boolean tryLock() {
        return sync.tryAcquire(1);
    }
    @Override
// 尝试，不成功，进入等待队列，有时限
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return sync.tryAcquireNanos(1, unit.toNanos(time));
    }
    @Override
// 释放锁
    public void unlock() {
        sync.release(1);
    }
    @Override
// 生成条件变量
    public Condition newCondition() {
        return sync.newCondition();
    }

}
