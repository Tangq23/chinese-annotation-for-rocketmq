package org.apache.rocketmq.example.scheduledmessage;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

public class ScheduledMessageProducer {
    public static void main(String[] args) throws Exception {
        /*
         * 实例化DefaultMQProducer，这里如要设置生产者组名。
         */
        DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName_scheduled");
        /*
         * 可以通过下面这句话来设置name server的地址，当然你也可以通过环境变量来进行设置
         * 因为我们在前面已经设置了环境变量，所以这里应该将这句话注释掉
         */
        producer.setNamesrvAddr("192.168.35.128:9876");
        /*
         * 启动实例
         */
        producer.start();
        int totalMessagesToSend = 100;
        // 发送100个消息
        for (int i = 0; i < totalMessagesToSend; i++) {
            Message message = new Message("Topic_scheduled", ("Hello scheduled message " + i).getBytes());
            /*
             * 这里是延时消息的关键。这只消息的延时级别。传入的参数表示是默认延时级别的第几个
             * 默认的延时级别：messageDelayLevel=1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
             * 所以当我们设置4时，消息大概延时时间是30s
             */
            message.setDelayTimeLevel(1);
            // 发送消息
            producer.send(message);
        }

        // 使用过后关闭生产者
        producer.shutdown();
    }
}
