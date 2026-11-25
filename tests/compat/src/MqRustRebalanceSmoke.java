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

/** R2 multi-consumer rebalance smoke: two clients, one group, 10 messages. */
public class MqRustRebalanceSmoke {
    private static DefaultMQPushConsumer consumer(String namesrv, String group, String topic,
        String instance, AtomicInteger count, CountDownLatch latch) throws Exception {
        DefaultMQPushConsumer c = new DefaultMQPushConsumer(group);
        c.setNamesrvAddr(namesrv);
        c.setInstanceName(instance);
        c.setVipChannelEnabled(false);
        c.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        c.subscribe(topic, "*");
        c.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (int i = 0; i < msgs.size(); i++) {
                count.incrementAndGet();
                System.out.println("REBALANCE " + instance + " recv "
                    + new String(msgs.get(i).getBody(), StandardCharsets.UTF_8));
                latch.countDown();
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        c.start();
        return c;
    }

    public static void main(String[] args) throws Exception {
        String namesrv = args.length > 0 ? args[0] : "127.0.0.1:9877";
        String topic = args.length > 1 ? args[1] : "MQ_RUST_REBALANCE";
        String group = "mq_rust_rebalance_group_" + System.currentTimeMillis();

        AtomicInteger c1Count = new AtomicInteger();
        AtomicInteger c2Count = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(10);
        DefaultMQPushConsumer c1 = consumer(namesrv, group, topic, "rebalance-1", c1Count, latch);
        DefaultMQPushConsumer c2 = consumer(namesrv, group, topic, "rebalance-2", c2Count, latch);
        Thread.sleep(5000);

        DefaultMQProducer producer = new DefaultMQProducer("mq_rust_rebalance_producer");
        producer.setNamesrvAddr(namesrv);
        producer.setVipChannelEnabled(false);
        producer.start();
        for (int i = 0; i < 10; i++) {
            producer.send(new Message(topic, ("rebalance-" + i).getBytes(StandardCharsets.UTF_8)));
        }
        producer.shutdown();

        boolean ok = latch.await(30, TimeUnit.SECONDS);
        Thread.sleep(1000);
        c1.shutdown();
        c2.shutdown();
        System.out.println("REBALANCE RESULT c1=" + c1Count.get() + " c2=" + c2Count.get() + " ok=" + ok);
        if (!ok || c1Count.get() == 0 || c2Count.get() == 0) {
            throw new IllegalStateException("rebalance smoke failed");
        }
    }
}
