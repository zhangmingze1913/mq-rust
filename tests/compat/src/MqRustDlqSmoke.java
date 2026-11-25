import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

/** R2 DLQ smoke: always fail with maxReconsumeTimes=1, then inspect %DLQ%group. */
public class MqRustDlqSmoke {
    public static void main(String[] args) throws Exception {
        String namesrv = args.length > 0 ? args[0] : "127.0.0.1:9877";
        String topic = args.length > 1 ? args[1] : "MQ_RUST_DLQ";
        String group = "mq_rust_dlq_group_" + System.currentTimeMillis();

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
        consumer.setNamesrvAddr(namesrv);
        consumer.setVipChannelEnabled(false);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setMaxReconsumeTimes(1);
        consumer.subscribe(topic, "*");
        AtomicInteger attempts = new AtomicInteger();
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt ignored : msgs) {
                System.out.println("DLQ ATTEMPT " + attempts.incrementAndGet());
            }
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        });
        consumer.start();
        Thread.sleep(3000);

        DefaultMQProducer producer = new DefaultMQProducer("mq_rust_dlq_producer");
        producer.setNamesrvAddr(namesrv);
        producer.setVipChannelEnabled(false);
        producer.start();
        producer.send(new Message(topic, "hello-dlq".getBytes(StandardCharsets.UTF_8)));
        producer.shutdown();

        // Let the retry loop reach the DLQ (push rebalance can take ~20s here).
        TimeUnit.SECONDS.sleep(45);
        consumer.shutdown();

        String dlqTopic = "%DLQ%" + group;
        DefaultMQPullConsumer pull = new DefaultMQPullConsumer("mq_rust_dlq_pull_" + System.currentTimeMillis());
        pull.setNamesrvAddr(namesrv);
        pull.setVipChannelEnabled(false);
        pull.start();
        Set<MessageQueue> queues = pull.fetchSubscribeMessageQueues(dlqTopic);
        int dlqCount = 0;
        for (MessageQueue mq : queues) {
            long offset = 0;
            PullResult result = pull.pull(mq, "*", offset, 32);
            if (result.getMsgFoundList() != null) {
                for (MessageExt msg : result.getMsgFoundList()) {
                    dlqCount++;
                    System.out.println("DLQ RECV " + new String(msg.getBody(), StandardCharsets.UTF_8)
                        + " reconsume=" + msg.getReconsumeTimes());
                }
            }
        }
        pull.shutdown();
        System.out.println("DLQ RESULT attempts=" + attempts.get() + " dlq=" + dlqCount
            + " topic=" + dlqTopic);
        if (attempts.get() < 2 || dlqCount < 1) {
            throw new IllegalStateException("dlq smoke failed");
        }
    }
}
