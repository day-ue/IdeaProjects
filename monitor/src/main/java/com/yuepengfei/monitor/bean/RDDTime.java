package com.yuepengfei.monitor.bean;

import java.io.Serializable;

public class RDDTime implements Serializable {
    public Long start;
    public Long end;

    public RDDTime() {
        this.start = System.currentTimeMillis();
        this.end = System.currentTimeMillis();
    }

    public RDDTime(Long start, Long end) {
        this.start = start;
        this.end = end;
    }

    public void add(Long time){
        this.start = this.end;
        this.end = time;
    }

}
