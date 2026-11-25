import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * SDK-level Inbox demo: three sends, same business key, only one process.
 */
public class MqRustInboxSmoke {
    private static boolean mark(String admin, String group, String key) throws Exception {
        URL url = new URL("http://" + admin + "/inbox/" + group + "/" + key);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        try (OutputStream out = conn.getOutputStream()) {
            out.write(new byte[0]);
        }
        String body = new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        return body.contains("\"duplicate\":false");
    }

    public static void main(String[] args) throws Exception {
        String namesrv = args.length > 0 ? args[0] : "127.0.0.1:9877";
        String admin = args.length > 1 ? args[1] : "127.0.0.1:9091";
        String topic = args.length > 2 ? args[2] : "MQ_RUST_INBOX";
        String group = "mq_rust_inbox_group_" + System.currentTimeMillis();

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
        consumer.setNamesrvAddr(namesrv);
        consumer.setVipChannelEnabled(false);
        AtomicInteger processed = new AtomicInteger();
        CountDownLatch seen = new CountDownLatch(3);
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                try {
                    String key = new String(msg.getBody(), StandardCharsets.UTF_8);
                    if (mark(admin, "inbox-demo", key)) {
                        processed.incrementAndGet();
                    }
                    seen.countDown();
                } catch (Exception e) {
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.subscribe(topic, "*");
        consumer.start();

        DefaultMQProducer producer = new DefaultMQProducer("mq_rust_inbox_producer");
        producer.setNamesrvAddr(namesrv);
        producer.setVipChannelEnabled(false);
        producer.start();
        for (int i = 0; i < 3; i++) {
            producer.send(new Message(topic, "business-1".getBytes(StandardCharsets.UTF_8)));
        }
        producer.shutdown();

        boolean done = seen.await(20, TimeUnit.SECONDS);
        consumer.shutdown();
        System.out.println("INBOX RESULT delivered=3 processed=" + processed.get() + " done=" + done);
        if (!done || processed.get() != 1) {
            throw new IllegalStateException("inbox smoke failed");
        }
    }
}
