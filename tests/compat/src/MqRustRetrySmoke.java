import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/** R2 retry smoke: first delivery returns RECONSUME_LATER, second succeeds. */
public class MqRustRetrySmoke {
    public static void main(String[] args) throws Exception {
        String namesrv = args.length > 0 ? args[0] : "127.0.0.1:9877";
        String topic = args.length > 1 ? args[1] : "MQ_RUST_RETRY";
        String group = "mq_rust_retry_group_" + System.currentTimeMillis();

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
        consumer.setNamesrvAddr(namesrv);
        consumer.setVipChannelEnabled(false);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(topic, "*");
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger attempts = new AtomicInteger();
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                int n = attempts.incrementAndGet();
                System.out.println("RETRY ATTEMPT " + n
                    + " reconsume=" + msg.getReconsumeTimes()
                    + " body=" + new String(msg.getBody(), StandardCharsets.UTF_8));
                if (msg.getReconsumeTimes() == 0) {
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
                latch.countDown();
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        Thread.sleep(3000);

        DefaultMQProducer producer = new DefaultMQProducer("mq_rust_retry_producer");
        producer.setNamesrvAddr(namesrv);
        producer.setVipChannelEnabled(false);
        producer.start();
        System.out.println("RETRY SEND " + producer.send(
            new Message(topic, "hello-retry".getBytes(StandardCharsets.UTF_8))));
        producer.shutdown();

        boolean ok = latch.await(30, TimeUnit.SECONDS);
        consumer.shutdown();
        System.out.println("RETRY RESULT attempts=" + attempts.get() + " ok=" + ok);
        if (!ok || attempts.get() < 2) {
            throw new IllegalStateException("retry smoke failed");
        }
    }
}
