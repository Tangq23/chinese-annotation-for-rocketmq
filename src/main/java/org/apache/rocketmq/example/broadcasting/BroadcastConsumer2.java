package org.apache.rocketmq.example.broadcasting;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

public class BroadcastConsumer2 {
    public static void main(String[] args) throws Exception {
        /*
         * 实例化DefaultMQPushConsumer，同样这里需要设置消费者组名
         */
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("ConsumerGroupName_broadcast");

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
         * 设置消费模式为broadcasting模式，默认情况下为集群模式，即MessageModel.CLUSTERING
         * MessageModel是一个枚举类，里面有两种实例：BROADCASTING和CLUSTERING，分别对应广播模式和集群模式
         * 广播模式与集群模式的关键区别在于：如果是集群模式，那么broker中的消息只能够被消费者集群中的其中一个消费者消费
         * 而如果是广播模式，那么整个消费者集群中的所有消费者都能够消费同一个消息，也就是说广播模式下消息能够被消费多次
         */
        consumer.setMessageModel(MessageModel.BROADCASTING);

        /*
         * 指定要订阅的topic来进行消费
         */
        consumer.subscribe("Topic_broadcast", "TagA || TagC || TagD");

        /*
         *  注册一个回调函数，这个回调函数会在消息从broker中取过来的时候调用执行
         */
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                System.out.printf(Thread.currentThread().getName() + " Receive New Messages: " + msgs + "%n");
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        /*
         *  开启消费者实例
         */
        consumer.start();
        System.out.printf("Broadcast Consumer Started.%n");
    }
}
