package org.apache.rocketmq.example.scheduledmessage;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class ScheduledMessageConsumer {
    public static void main(String[] args) throws Exception {
        /*
         * 实例化DefaultMQPushConsumer，同样这里需要设置消费者组名
         */
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("ConsumerGroupName_scheduled");
        /*
         * 可以通过下面这句话来设置name server的地址，当然你也可以通过环境变量来进行设置
         * 因为我们在前面已经设置了环境变量，所以这里应该将这句话注释掉
         */
        consumer.setNamesrvAddr("192.168.35.128:9876");
        /*
         * 指定要订阅的topic来进行消费
         */
        consumer.subscribe("Topic_scheduled", "*");
        /*
         *  注册一个回调函数，这个回调函数会在消息从broker中取过来的时候调用执行
         */
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messages, ConsumeConcurrentlyContext context) {
                for (MessageExt message : messages) {
                    // 打印大致的延时时间，这里需要注意，是当前时间减去消息生成时间，而不是存储时间，官方示例代码写错了
                    System.out.println("Receive message[msgId=" + message.getMsgId() + "] "
                            + (System.currentTimeMillis() - message.getBornTimestamp()) + "ms later");
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        /*
         *  开启消费者实例
         */
        consumer.start();
    }
}
