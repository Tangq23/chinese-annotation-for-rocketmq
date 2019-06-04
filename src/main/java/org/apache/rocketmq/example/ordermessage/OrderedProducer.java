package org.apache.rocketmq.example.ordermessage;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;

public class OrderedProducer {
    public static void main(String[] args) throws Exception {
        // 实例化DefaultMQProducer，这里如要设置生产者组名。
        DefaultMQProducer producer = new DefaultMQProducer("example_group_name");
        // 指明name server的地址
        producer.setNamesrvAddr("192.168.35.128:9876");
        // 启动实例
        producer.start();
        // 发送100个消息，每个消息
        for (int i = 0; i < 100; i++) {
            int orderId = i % 10;
            //创建消息实例，指定topic、tag和消息主体，这里将topic省略了，因为它并不是必须的
            Message msg = new Message("TopicOrderTest", ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            /*
             * 调用send方法来发送消息，这里的send方法中第二个参数传递了一个队列选择器，是一个匿名内部类，实现了select方法
             * 在select方法中，第二第三个参数Message msg, Object arg就是send方法中的第一和第三个参数的值
             * 我们将订单号为0,4,8的消息放到第0个队列中，订单号为1,5,9的消息放到第1个队列中
             * 将订单号为2,6的消息放到第2个队列中，订单号为3,7的消息放到第3个队列中
             * 由于每一个订单号下对应着10个消息，那么在四个队列中分别存放了30，30，20，20个消息
             */
            SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    Integer id = (Integer) arg;
                    return mqs.get(id % mqs.size());
                }
            }, orderId);

            // 将发送消息的结果打印下来
            System.out.printf("%s%n", sendResult);
        }
        // 最后关闭生产者producer，只需关闭一次
        producer.shutdown();
    }
}
