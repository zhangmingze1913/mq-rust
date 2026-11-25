import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * MQ-rust R1 compatibility smoke against the official RocketMQ 4.9.8 Java SDK.
 *
 * Usage: java MqRustCompatSmoke 127.0.0.1:9877 MQ_RUST_SMOKE
 */
public class MqRustCompatSmoke {
    public static void main(String[] args) throws Exception {
        String namesrv = args.length > 0 ? args[0] : "127.0.0.1:9877";
        String topic = args.length > 1 ? args[1] : "MQ_RUST_SMOKE";

        DefaultMQProducer producer = new DefaultMQProducer("mq_rust_producer_group");
        producer.setNamesrvAddr(namesrv);
        producer.setVipChannelEnabled(false);
        producer.setSendMsgTimeout(5000);
        producer.start();

        SendResult sendResult = null;
        for (int i = 0; i < 5; i++) {
            Message msg = new Message(topic, "TagA", "sync-" + i,
                ("hello-mq-rust-sync-" + i).getBytes(StandardCharsets.UTF_8));
            sendResult = producer.send(msg);
            System.out.println("SEND sync " + i + " -> " + sendResult);
        }

        CountDownLatch asyncLatch = new CountDownLatch(5);
        AtomicInteger asyncOk = new AtomicInteger();
        for (int i = 0; i < 5; i++) {
            Message msg = new Message(topic, "TagA", "async-" + i,
                ("hello-mq-rust-async-" + i).getBytes(StandardCharsets.UTF_8));
            producer.send(msg, new SendCallback() {
                @Override
                public void onSuccess(SendResult result) {
                    asyncOk.incrementAndGet();
                    asyncLatch.countDown();
                }

                @Override
                public void onException(Throwable e) {
                    System.out.println("ASYNC FAILED " + e);
                    asyncLatch.countDown();
                }
            });
        }
        asyncLatch.await(10, TimeUnit.SECONDS);

        for (int i = 0; i < 5; i++) {
            Message msg = new Message(topic, "TagA", "oneway-" + i,
                ("hello-mq-rust-oneway-" + i).getBytes(StandardCharsets.UTF_8));
            producer.sendOneway(msg);
        }
        Thread.sleep(300);
        producer.shutdown();

        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("mq_rust_consumer_group");
        consumer.setNamesrvAddr(namesrv);
        consumer.setVipChannelEnabled(false);
        consumer.start();

        Set<MessageQueue> queues = consumer.fetchSubscribeMessageQueues(topic);
        System.out.println("QUEUES " + queues.size());
        int received = 0;
        for (MessageQueue mq : queues) {
            long offset = consumer.fetchConsumeOffset(mq, false);
            if (offset < 0) {
                offset = 0;
            }
            for (int round = 0; round < 8; round++) {
                PullResult pullResult = consumer.pull(mq, "*", offset, 32);
                System.out.println("PULL " + mq + " status=" + pullResult.getPullStatus()
                    + " offset=" + offset
                    + " next=" + pullResult.getNextBeginOffset()
                    + " count=" + (pullResult.getMsgFoundList() == null ? 0 : pullResult.getMsgFoundList().size()));
                if (pullResult.getMsgFoundList() == null || pullResult.getMsgFoundList().isEmpty()) {
                    break;
                }
                for (MessageExt msg : pullResult.getMsgFoundList()) {
                    received++;
                    System.out.println("RECV queue=" + msg.getQueueId()
                        + " offset=" + msg.getQueueOffset()
                        + " body=" + new String(msg.getBody(), StandardCharsets.UTF_8)
                        + " tags=" + msg.getTags());
                }
                offset = pullResult.getNextBeginOffset();
                if (offset >= pullResult.getMaxOffset()) {
                    break;
                }
            }
        }
        consumer.shutdown();

        System.out.println("RESULT received=" + received
            + " asyncOk=" + asyncOk.get()
            + " send=" + (sendResult != null));
        if (sendResult == null || asyncOk.get() != 5 || received < 15) {
            throw new IllegalStateException("compat smoke failed");
        }
    }
}
