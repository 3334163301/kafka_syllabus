package com.atguigu.kafka_syllabus.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by Administrator on 2019/7/4.
 */
public class CustomConsumer1 {

    public static void main(String[] Args){


        //1.配置消费者属性
        Properties props = new Properties();
        //配置kafka集群节点的地址，可以是多个
        props.put("bootstrap.servers","hadoop102:9092");
        //设置消费组
        props.put("group.id","g1");
        //是否自动确认offset
        props.put("enable.auto.commit","true");
        //key的反序列化
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        //value的反序列化
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        //2.创建消费者实例
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        //4.释放资源---当java虚拟机即将shutdown的时候，会另外开启一个线程回收资源
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                if(consumer !=null){
                    System.out.println("释放资源---当java虚拟机即将shutdown的时候，会另外开启一个线程回收资源");
                    consumer.close();
                }
            }
        }));
        //3.订阅消息主题
        consumer.subscribe(Arrays.asList("test2"));
        //生产环境下，应该定义一个flag,监控Hbase集群的健康状态，如果Hbase集群正常flag=true
        int i=0;
        while(true){
            i++;
            ConsumerRecords<String,String> records = consumer.poll(100);
            for (ConsumerRecord<String,String> record:records){
                System.out.println(record.offset()+"----"+record.key()+"-----"+record.value());
            }
            //测试shutdown资源的回收
//            if(i==20){
//                break;
//            }
        }
        //这里是无法释放资源的，因为上面的while(true)，所以程序永远都不会执行到这里来
       // System.out.println("while(true)里面有结束条件，所以编译又通过了");
    }
}
