package org.apache.rocketmq.example.simplemessage;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class AsyncProducer {
    public static void main(String[] args) throws Exception {
        //实例化DefaultMQProducer，这里如要设置生产者组名。
        DefaultMQProducer producer = new DefaultMQProducer("a_synchronization_producer_group");
        //指明name server的地址
        producer.setNamesrvAddr("192.168.35.128:9876");
        //启动producer实例，只需要启动一次
        producer.start();
        //当异步发送失败，内部执行的最大重发次数，内部执行重发可能导致消息重复，这里设置为0
        producer.setRetryTimesWhenSendAsyncFailed(0);
        int messageCount = 100;
        final CountDownLatch countDownLatch = new CountDownLatch(messageCount);
        for (int i = 0; i < messageCount; i++) {
            try {
                final int index = i;
                //创建消息实例，指定topic、tag和消息主体
                Message msg = new Message("Jodie_topic_1023",
                        "TagA",
                        "OrderID188",
                        "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));
                // 异步发送消息, 该方法立即返回。当发送完成之后sendCallback将会被执行
                producer.send(msg, new SendCallback() {
                    public void onSuccess(SendResult sendResult) {
                        // 消费发送成功
                        countDownLatch.countDown();
                        System.out.printf("%-10d OK %s %n", index, sendResult.getMsgId());
                    }
                    public void onException(Throwable e) {
                        // 消息发送失败
                        countDownLatch.countDown();
                        System.out.printf("%-10d Exception %s %n", index, e);
                        e.printStackTrace();
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        //等待异步回调被执行，或者5秒之内还没有被执行完成，结束等待
        countDownLatch.await(5, TimeUnit.SECONDS);
        //最后关闭生产者producer，只需关闭一次
        producer.shutdown();
    }
}
