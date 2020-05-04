package com.yuepengfei.script

import com.yuepengfei.bean.LoginEvent
import org.apache.flink.cep.pattern.Pattern
import org.apache.flink.cep.pattern.conditions.SimpleCondition
import org.apache.flink.streaming.api.windowing.time.Time

class Rule implements Serializable{
    def run() {
        Pattern<LoginEvent, ?> pattern =Pattern.<LoginEvent>begin("begin")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    boolean filter(LoginEvent loginEvent) throws Exception {
                        return loginEvent.getType_type().equals("fail")
                    }
                })
                .next("next")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    boolean filter(LoginEvent loginEvent) throws Exception {
                        return loginEvent.getType_type().equals("fail")
                    }
                }).within(Time.seconds(1))
        return pattern
    }
}
