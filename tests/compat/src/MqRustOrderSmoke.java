import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

/** R6 ordered messages: same queue -> consumer sees 0..N-1 in order. */
public class MqRustOrderSmoke {
    public static void main(String[] args) throws Exception {
        String namesrv = args.length > 0 ? args[0] : "127.0.0.1:9877";
        String topic = args.length > 1 ? args[1] : "MQ_RUST_ORDER";
        int total = 50;

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("mq_rust_order_consumer");
        consumer.setNamesrvAddr(namesrv);
        consumer.setVipChannelEnabled(false);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(topic, "*");
        List<Integer> received = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(total);
        consumer.registerMessageListener((MessageListenerOrderly) (msgs, context) -> {
            for (var msg : msgs) {
                String body = new String(msg.getBody(), StandardCharsets.UTF_8);
                received.add(Integer.parseInt(body.substring(body.lastIndexOf('-') + 1)));
                latch.countDown();
            }
            return ConsumeOrderlyStatus.SUCCESS;
        });
        consumer.start();
        Thread.sleep(3000);

        DefaultMQProducer producer = new DefaultMQProducer("mq_rust_order_producer");
        producer.setNamesrvAddr(namesrv);
        producer.setVipChannelEnabled(false);
        producer.start();
        Thread.sleep(1500);
        MessageQueueSelector selector = (mqs, msg, arg) -> mqs.get(0);
        for (int i = 0; i < total; i++) {
            producer.send(new Message(topic, ("order-" + i).getBytes(StandardCharsets.UTF_8)),
                selector, null);
        }
        producer.shutdown();

        boolean done = latch.await(30, TimeUnit.SECONDS);
        consumer.shutdown();
        boolean ordered = true;
        for (int i = 0; i < received.size(); i++) {
            if (received.get(i) != i) {
                ordered = false;
                break;
            }
        }
        System.out.println("ORDER RESULT done=" + done + " count=" + received.size()
            + " ordered=" + ordered);
        if (!done || received.size() != total || !ordered) {
            throw new IllegalStateException("order smoke failed");
        }
    }
}
