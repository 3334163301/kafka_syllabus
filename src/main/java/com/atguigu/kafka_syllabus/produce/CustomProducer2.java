package com.atguigu.kafka_syllabus.produce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * 接口回调数据生产者
 * Created by Administrator on 2019/7/4.
 */
public class CustomProducer2 {
    public static  void main(String [] Args){
        //1.配置生产者属性
        Properties props = new Properties();
        //配置kafka集群节点的地址，可以是多个
        props.put("bootstrap.servers","hadoop102:9092");
        //配置发送的消息是否等待应答
        props.put("acks","all");
        //配置消息发送失败的重试次数
        props.put("retries","0");
        //批处理数据的大小
        props.put("batch.size","16384");
        //设置批处理数据的延迟，单位：ms
        props.put("linger.ms","3");
        //设置内存缓冲区的大小
        props.put("buffer.memory","33554432");
        //数据在发送之前一定要序列化
        //key
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        //value
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        //设置分区规则
        props.put("partitioner.class","com.atguigu.kafka_syllabus.partitioner.CustomPartitioner1");

        //2.实例化KafkaProducer
        KafkaProducer<String,String> producer = new KafkaProducer<>(props);

        for (int i=0;i<50;i++){
            //调用producer的send方法，进行消息的发送，每条待发送的消息--都必须封装为一个record对象
            producer.send(new ProducerRecord<String, String>("test2", "helloPartition" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(recordMetadata != null){
                        System.out.println(recordMetadata.offset()+"-------"+recordMetadata.partition());
                    }
                }
            });

        }
        //close 以释放资源
        producer.close();

    }
}
