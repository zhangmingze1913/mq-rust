import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;

/** R2 SQL92 push filter: only price > 100 is delivered. */
public class MqRustSql92PushSmoke {
    public static void main(String[] args) throws Exception {
        String namesrv = args.length > 0 ? args[0] : "127.0.0.1:9877";
        String topic = args.length > 1 ? args[1] : "MQ_RUST_SQL92_PUSH";

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("mq_rust_sql92_push_group");
        consumer.setNamesrvAddr(namesrv);
        consumer.setVipChannelEnabled(false);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(topic, MessageSelector.bySql("price > 100"));
        AtomicInteger count = new AtomicInteger();
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (var msg : msgs) {
                count.incrementAndGet();
                System.out.println("SQL92 PUSH RECV "
                    + new String(msg.getBody(), StandardCharsets.UTF_8)
                    + " price=" + msg.getUserProperty("price"));
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        Thread.sleep(3000);

        DefaultMQProducer producer = new DefaultMQProducer("mq_rust_sql92_push_producer");
        producer.setNamesrvAddr(namesrv);
        producer.setVipChannelEnabled(false);
        producer.start();
        Message cheap = new Message(topic, "cheap".getBytes(StandardCharsets.UTF_8));
        cheap.putUserProperty("price", "50");
        Message expensive = new Message(topic, "expensive".getBytes(StandardCharsets.UTF_8));
        expensive.putUserProperty("price", "150");
        producer.send(cheap);
        producer.send(expensive);
        producer.shutdown();

        TimeUnit.SECONDS.sleep(8);
        consumer.shutdown();
        System.out.println("SQL92 PUSH RESULT count=" + count.get());
        if (count.get() != 1) {
            throw new IllegalStateException("sql92 push smoke failed");
        }
    }
}
