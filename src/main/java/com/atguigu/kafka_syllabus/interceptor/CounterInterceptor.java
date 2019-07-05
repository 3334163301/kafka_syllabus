package com.atguigu.kafka_syllabus.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * Created by Administrator on 2019/7/5.
 */
public class CounterInterceptor implements ProducerInterceptor<String,String> {
    private long successCount=0;
    private long errorCount=0;
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        return producerRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if(e==null){
            successCount++;
        }else {
            errorCount++;
        }

    }

    @Override
    public void close() {
        System.out.println("成功处理的个数"+successCount);
        System.out.println("处理失败的个数"+successCount);

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
