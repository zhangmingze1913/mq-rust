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

/** R2 Tag filter smoke: subscribe TagA, send 3 TagA + 3 TagB, expect 3. */
public class MqRustTagSmoke {
    public static void main(String[] args) throws Exception {
        String namesrv = args.length > 0 ? args[0] : "127.0.0.1:9877";
        String topic = args.length > 1 ? args[1] : "MQ_RUST_TAG";

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(
            "mq_rust_tag_group_" + System.currentTimeMillis());
        consumer.setNamesrvAddr(namesrv);
        consumer.setVipChannelEnabled(false);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(topic, "TagA");
        CountDownLatch latch = new CountDownLatch(3);
        AtomicInteger received = new AtomicInteger();
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                received.incrementAndGet();
                System.out.println("TAG RECV tags=" + msg.getTags()
                    + " body=" + new String(msg.getBody(), StandardCharsets.UTF_8));
                latch.countDown();
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        Thread.sleep(3000);

        DefaultMQProducer producer = new DefaultMQProducer("mq_rust_tag_producer");
        producer.setNamesrvAddr(namesrv);
        producer.setVipChannelEnabled(false);
        producer.start();
        for (int i = 0; i < 3; i++) {
            producer.send(new Message(topic, "TagA", ("A-" + i).getBytes(StandardCharsets.UTF_8)));
            producer.send(new Message(topic, "TagB", ("B-" + i).getBytes(StandardCharsets.UTF_8)));
        }
        producer.shutdown();

        boolean ok = latch.await(20, TimeUnit.SECONDS);
        Thread.sleep(1000);
        consumer.shutdown();
        System.out.println("TAG RESULT received=" + received.get() + " ok=" + ok);
        if (!ok || received.get() != 3) {
            throw new IllegalStateException("tag filter smoke failed");
        }
    }
}
