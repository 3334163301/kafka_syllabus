package com.atguigu.kafka_syllabus.stream;



import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * Created by Administrator on 2019/7/5.
 */
public class LogProcessor implements Processor<byte[],byte[]> {
    private ProcessorContext processorContext;
    @Override
    public void init(ProcessorContext processorContext) {
        this.processorContext=processorContext;

    }
/*参数是每一条消息的key和value*/
    @Override
    public void process(byte[] key, byte[] value) {
        String info =new String(value);
        if (info.contains(">>>")){
            info = info.split(">>>")[1];
        }
        /*因为要被Hbase消费，所以必须是二进制*/
        processorContext.forward(key,info.getBytes());

    }
/*周期性调度这个方法*/
    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }
}
