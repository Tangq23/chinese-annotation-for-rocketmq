package org.apache.rocketmq.example.ordermessage;


import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class OrderedConsumer {
    public static void main(String[] args) throws Exception {
        // 实例化DefaultMQPushConsumer，同样这里需要设置消费者组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("example_consumerGroup_name");
        /*
         * 可以通过下面这句话来设置name server的地址，当然你也可以通过环境变量来进行设置
         * 因为我们在前面已经设置了环境变量，所以这里应该将这句话注释掉
         */
        consumer.setNamesrvAddr("192.168.35.128:9876");
        /*
         * 在指定的消费者组是全新的情况下，指定从哪里开始消费。
         * ConsumeFromWhere是一个枚举类
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        /*
         * 指定要订阅的topic来进行消费，后面的表达式可以通过消息中的tag来对消息进行过滤
         */
        consumer.subscribe("TopicOrderTest", "*");
        /*
         * 注册一个回调函数，这个回调函数会在消息从broker中取过来的时候调用执行
         * 这里有一点非常关键，也是决定了这种方式是顺序消费消息的根本
         * 这里注册的消息监听器是MessageListenerOrderly，注意与MessageListenerConcurrently的区别
         * 使用前者就能够保证同一个线程总是从同一个队列中去取消息，而后者则不能保证这一点
         * 所以，通过生产者和消费者的配合，生产者将同一个订单号下的所有消息都放在同一个队列中
         * 而消费者端，同一个线程总是消费同一个队列中的消息，这样我们就能够保证，每一个订单号下面的所有消息被同一个线程消费
         * 也就是顺序消息的机制
         */
        consumer.registerMessageListener(new MessageListenerOrderly() {
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        //开启消费者实例
        consumer.start();

        System.out.printf("Consumer Started.%n");
    }
}
