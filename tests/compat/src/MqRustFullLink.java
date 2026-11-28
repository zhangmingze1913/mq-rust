import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

public final class MqRustFullLink {
    public static void main(String[] args) throws Exception {
        String namesrv = args.length > 0 ? args[0] : "127.0.0.1:9877";
        String topic = args.length > 1 ? args[1] : "FULL_LINK";
        int sendThreads = args.length > 2 ? Integer.parseInt(args[2]) : 4;
        int recvThreads = args.length > 3 ? Integer.parseInt(args[3]) : 4;
        int seconds = args.length > 4 ? Integer.parseInt(args[4]) : 60;
        int batch = args.length > 5 ? Integer.parseInt(args[5]) : 256;
        int sendTimeoutMs = args.length > 6 ? Integer.parseInt(args[6]) : 3000;

        AtomicLong sent = new AtomicLong();
        AtomicLong failed = new AtomicLong();
        AtomicLong received = new AtomicLong();
        ConcurrentLinkedQueue<Long> latencies = new ConcurrentLinkedQueue<>();
        long deadline = System.currentTimeMillis() + seconds * 1000L;

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("full_link_consumer");
        consumer.setNamesrvAddr(namesrv);
        consumer.setConsumeThreadMin(recvThreads);
        consumer.setConsumeThreadMax(recvThreads);
        consumer.subscribe(topic, "*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messages,
                    ConsumeConcurrentlyContext context) {
                long now = System.currentTimeMillis();
                for (MessageExt message : messages) {
                    received.incrementAndGet();
                    String body = new String(message.getBody(), StandardCharsets.UTF_8);
                    int split = body.indexOf(':');
                    if (split > 0) {
                        try {
                            long sentAt = Long.parseLong(body.substring(0, split));
                            latencies.add(now - sentAt);
                        } catch (NumberFormatException ignored) {
                        }
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();

        DefaultMQProducer producer = new DefaultMQProducer("full_link_producer");
        producer.setNamesrvAddr(namesrv);
        producer.setSendMsgTimeout(sendTimeoutMs);
        producer.setMaxMessageSize(4 * 1024 * 1024);
        producer.start();

        ExecutorService pool = Executors.newFixedThreadPool(sendThreads);
        byte[] payload = new byte[240];
        java.util.Arrays.fill(payload, (byte) 'x');
        for (int thread = 0; thread < sendThreads; thread++) {
            final int id = thread;
            pool.submit(() -> {
                while (System.currentTimeMillis() < deadline) {
                    Collection<Message> messages = new ArrayList<>(batch);
                    for (int index = 0; index < batch; index++) {
                        long stamp = System.currentTimeMillis();
                        String body = stamp + ":" + id + ":" + index + ":"
                                + new String(payload, StandardCharsets.UTF_8);
                        messages.add(new Message(topic, body.getBytes(StandardCharsets.UTF_8)));
                    }
                    try {
                        producer.send(messages);
                        sent.addAndGet(batch);
                    } catch (Exception err) {
                        failed.addAndGet(batch);
                    }
                }
            });
        }
        pool.shutdown();
        pool.awaitTermination(seconds + 10, TimeUnit.SECONDS);
        Thread.sleep(3000);
        consumer.shutdown();
        producer.shutdown();

        List<Long> values = new ArrayList<>(latencies);
        java.util.Collections.sort(values);
        long p50 = values.isEmpty() ? 0 : values.get(values.size() / 2);
        long p99 = values.isEmpty() ? 0 : values.get(Math.min(values.size() - 1, values.size() * 99 / 100));
        System.out.println("FULL_LINK_RESULT sent=" + sent.get()
                + " failed=" + failed.get()
                + " received=" + received.get()
                + " p50ms=" + p50
                + " p99ms=" + p99
                + " seconds=" + seconds);
    }
}
