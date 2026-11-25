import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;

/** Push-consume with a fixed group so mqadmin consumerProgress can be checked. */
public class MqRustOffsetProgress {
    public static void main(String[] args) throws Exception {
        String namesrv = args.length > 0 ? args[0] : "127.0.0.1:9877";
        String topic = args.length > 1 ? args[1] : "MQ_OFFSET_PROGRESS";
        String group = args.length > 2 ? args[2] : "mq_rust_offset_progress_group";

        DefaultMQProducer producer = new DefaultMQProducer("mq_rust_offset_progress_producer");
        producer.setNamesrvAddr(namesrv);
        producer.setVipChannelEnabled(false);
        producer.start();
        for (int i = 0; i < 5; i++) {
            producer.send(new Message(topic, ("m" + i).getBytes(StandardCharsets.UTF_8)));
        }
        producer.shutdown();

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
        consumer.setNamesrvAddr(namesrv);
        consumer.setVipChannelEnabled(false);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(topic, "*");
        AtomicInteger count = new AtomicInteger();
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            count.addAndGet(msgs.size());
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        Thread.sleep(5000);
        consumer.shutdown();
        System.out.println("OFFSET_PROGRESS consumed=" + count.get() + " group=" + group);
        if (count.get() < 5) {
            throw new IllegalStateException("consume failed");
        }
    }
}
