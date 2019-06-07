package org.apache.rocketmq.example.broadcasting;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class BroadcastProducer {
    public static void main(String[] args) throws Exception {
        /*
         * 实例化DefaultMQProducer，这里如要设置生产者组名。
         */
        DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName_broadcast");
        /*
         * 可以通过下面这句话来设置name server的地址，当然你也可以通过环境变量来进行设置
         * 因为我们在前面已经设置了环境变量，所以这里应该将这句话注释掉
         */
        producer.setNamesrvAddr("192.168.35.128:9876");
        /*
         * 启动实例
         */
        producer.start();

        // 发送100个消息
        for (int i = 0; i < 100; i++) {
            Message msg = new Message("Topic_broadcast",
                    "TagA",
                    "OrderID188",
                    "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
        }
        /*
         * 最后关闭生产者producer，只需关闭一次
         */
        producer.shutdown();
    }
}
