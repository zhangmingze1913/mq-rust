import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;

/** Send a __STARTDELIVERTIME message, restart broker, then consume it. */
public class MqRustStartDeliverRestart {
    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0] : "send";
        String namesrv = args.length > 1 ? args[1] : "127.0.0.1:9877";
        String topic = args.length > 2 ? args[2] : "MQ_RUST_START_RESTART";
        long delayMs = args.length > 3 ? Long.parseLong(args[3]) : 8000;

        if (mode.equals("send")) {
            DefaultMQProducer producer = new DefaultMQProducer("mq_rust_start_restart_producer");
            producer.setNamesrvAddr(namesrv);
            producer.setVipChannelEnabled(false);
            producer.start();
            Message msg = new Message(topic, "restart-delayed".getBytes(StandardCharsets.UTF_8));
            long due = System.currentTimeMillis() + delayMs;
            msg.putUserProperty("__STARTDELIVERTIME", String.valueOf(due));
            producer.send(msg);
            producer.shutdown();
            System.out.println("START_RESTART SENT due=" + due);
            return;
        }

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("mq_rust_start_restart_group");
        consumer.setNamesrvAddr(namesrv);
        consumer.setVipChannelEnabled(false);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(topic, "*");
        AtomicInteger count = new AtomicInteger();
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            count.addAndGet(msgs.size());
            for (var msg : msgs) {
                System.out.println("START_RESTART RECV " + new String(msg.getBody(), StandardCharsets.UTF_8));
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        Thread.sleep(12000);
        consumer.shutdown();
        System.out.println("START_RESTART RESULT count=" + count.get());
        if (count.get() != 1) {
            throw new IllegalStateException("start-deliver restart failed");
        }
    }
}
