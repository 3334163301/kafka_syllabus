package com.atguigu.kafka_syllabus;

import com.atguigu.kafka_syllabus.stream.LogProcessor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

/**
 * Created by Administrator on 2019/7/6.
 */
public class App {
    public static void main(String[] Args){
        String fromTopic = "test2";
        String toTopic = "test3";

        //设置参数
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,"logProcessor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092,hadoop104:9092");

        //实例化StreamsConfig
        StreamsConfig config = new StreamsConfig(props);
        //构建tuopu
        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("SOURCE",fromTopic)
                .addProcessor("PROCESSOR", new ProcessorSupplier<byte[],byte[]>() {

                    @Override
                    public Processor<byte[], byte[]> get() {
                        return new LogProcessor();
                    }
                },"SOURCE")
                .addSink("SINK",toTopic,"PROCESSOR");

        //根据"Streamsconfig"以及用于构建拓扑的builder对象---实例化kafkaStreams
        KafkaStreams streams = new KafkaStreams(builder,config);
        streams.start();
    }
}
