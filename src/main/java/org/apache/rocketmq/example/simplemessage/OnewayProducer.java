package org.apache.rocketmq.example.simplemessage;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class OnewayProducer {
    public static void main(String[] args) throws Exception {
        // 实例化DefaultMQProducer，这里如要设置生产者组名。
        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");
        // 指明name server的地址
        producer.setNamesrvAddr("localhost:9876");
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
            // 单向模式直接发送消息而不等待broker的确认信息，这种方式具有最大的吞吐量，但是却有丢失信息的风险
            producer.sendOneway(msg);
        }
        //最后关闭生产者producer，只需关闭一次
        producer.shutdown();
    }
}
