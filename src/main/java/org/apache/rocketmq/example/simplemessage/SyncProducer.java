package org.apache.rocketmq.example.simplemessage;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class SyncProducer {
    public static void main(String[] args) throws Exception {
        // 实例化DefaultMQProducer，这里如要设置生产者组名。
        DefaultMQProducer producer = new DefaultMQProducer("synchronization_producer_group");
        // 指明name server的地址
        producer.setNamesrvAddr("192.168.35.128:9876");
        // 启动producer实例，只需要启动一次
        producer.start();
        //循环发送消息
        for (int i = 0; i < 100; i++) {
            //创建消息实例，指定topic、tag和消息主体
            Message msg = new Message("TopicTest" /* Message 所属的 Topic */,
                    "TagA" /* Message Tag 可理解为 Gmail 中的标签，对消息进行再归类，方便 Consumer 指定过滤条件在消息队列 RocketMQ 的服务器过滤 */,
                    ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET)
                    /*  Message Body 可以是任何二进制形式的数据， 消息队列 RocketMQ 不做任何干预，
                    需要 Producer 与 Consumer 协商好一致的序列化和反序列化方式 */
            );
            //调用send方法来将消息发送给其中一个broker
            SendResult sendResult = producer.send(msg);
            //同步发送消息，发送一个消息之后立马就能够收到响应
            System.out.printf("%s%n", sendResult);
        }
        //最后关闭生产者producer，只需关闭一次
        producer.shutdown();
    }
}
