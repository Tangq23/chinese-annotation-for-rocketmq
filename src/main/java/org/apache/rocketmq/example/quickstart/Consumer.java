package org.apache.rocketmq.example.quickstart;

import java.util.List;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * 下面这段代码展示了如何使用DefaultMQPushConsumer来进行消息的订阅和消费
 */
public class Consumer {

    public static void main(String[] args) throws InterruptedException, MQClientException {

        /*
         * 实例化DefaultMQPushConsumer，同样这里需要设置消费者组名
         */
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumerGroupName");

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
         * 指定要订阅的topic来进行消费
         */
        consumer.subscribe("testTopic", "*");

        /*
         *  注册一个回调函数，这个回调函数会在消息从broker中取过来的时候调用执行
         */
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        /*
         *  开启消费者实例
         */
        consumer.start();

        System.out.printf("Consumer Started.%n");
    }
}