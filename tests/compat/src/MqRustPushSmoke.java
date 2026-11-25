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

/** R2 push-consumer smoke: heartbeat, rebalance, long polling, offset commit. */
public class MqRustPushSmoke {
    public static void main(String[] args) throws Exception {
        String namesrv = args.length > 0 ? args[0] : "127.0.0.1:9877";
        String topic = args.length > 1 ? args[1] : "MQ_RUST_PUSH";
        int expected = args.length > 2 ? Integer.parseInt(args[2]) : 5;

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(
            "mq_rust_push_group_" + System.currentTimeMillis());
        consumer.setNamesrvAddr(namesrv);
        consumer.setVipChannelEnabled(false);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(topic, "*");

        CountDownLatch latch = new CountDownLatch(expected);
        AtomicInteger received = new AtomicInteger();
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                received.incrementAndGet();
                System.out.println("PUSH RECV queue=" + msg.getQueueId()
                    + " offset=" + msg.getQueueOffset()
                    + " body=" + new String(msg.getBody(), StandardCharsets.UTF_8)
                    + " reconsume=" + msg.getReconsumeTimes());
                latch.countDown();
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        System.out.println("PUSH consumer started");
        Thread.sleep(3000);

        DefaultMQProducer producer = new DefaultMQProducer("mq_rust_push_producer");
        producer.setNamesrvAddr(namesrv);
        producer.setVipChannelEnabled(false);
        producer.start();
        for (int i = 0; i < expected; i++) {
            Message msg = new Message(topic, "TagA", "push-" + i,
                ("hello-push-" + i).getBytes(StandardCharsets.UTF_8));
            System.out.println("PUSH SEND " + producer.send(msg));
        }
        producer.shutdown();

        boolean ok = latch.await(30, TimeUnit.SECONDS);
        // Give the push consumer's periodic offset persist a chance to run.
        Thread.sleep(6000);
        consumer.shutdown();
        System.out.println("PUSH RESULT received=" + received.get() + " expected=" + expected + " ok=" + ok);
        if (!ok || received.get() < expected) {
            throw new IllegalStateException("push smoke failed");
        }
    }
}
