import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

/** R2 batch smoke: send 5 messages as one MessageBatch, pull 5. */
public class MqRustBatchSmoke {
    public static void main(String[] args) throws Exception {
        String namesrv = args.length > 0 ? args[0] : "127.0.0.1:9877";
        String topic = args.length > 1 ? args[1] : "MQ_RUST_BATCH";

        DefaultMQProducer producer = new DefaultMQProducer("mq_rust_batch_producer");
        producer.setNamesrvAddr(namesrv);
        producer.setVipChannelEnabled(false);
        producer.start();
        List<Message> batch = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            batch.add(new Message(topic, "TagA", ("batch-" + i).getBytes(StandardCharsets.UTF_8)));
        }
        SendResult result = producer.send(batch);
        System.out.println("BATCH SEND " + result);
        producer.shutdown();

        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("mq_rust_batch_consumer");
        consumer.setNamesrvAddr(namesrv);
        consumer.setVipChannelEnabled(false);
        consumer.start();
        Set<MessageQueue> queues = consumer.fetchSubscribeMessageQueues(topic);
        int received = 0;
        for (MessageQueue mq : queues) {
            long offset = 0;
            for (int round = 0; round < 8; round++) {
                PullResult pull = consumer.pull(mq, "*", offset, 32);
                if (pull.getMsgFoundList() == null || pull.getMsgFoundList().isEmpty()) {
                    break;
                }
                for (MessageExt msg : pull.getMsgFoundList()) {
                    received++;
                    System.out.println("BATCH RECV queue=" + msg.getQueueId()
                        + " offset=" + msg.getQueueOffset()
                        + " body=" + new String(msg.getBody(), StandardCharsets.UTF_8));
                }
                offset = pull.getNextBeginOffset();
                if (offset >= pull.getMaxOffset()) {
                    break;
                }
            }
        }
        consumer.shutdown();
        System.out.println("BATCH RESULT received=" + received);
        if (received != 5) {
            throw new IllegalStateException("batch smoke failed");
        }
    }
}
