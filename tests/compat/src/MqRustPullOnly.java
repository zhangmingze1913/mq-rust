import java.nio.charset.StandardCharsets;
import java.util.Set;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

/** Pull-only check used to verify broker restart recovery (no new sends). */
public class MqRustPullOnly {
    public static void main(String[] args) throws Exception {
        String namesrv = args.length > 0 ? args[0] : "127.0.0.1:9877";
        String topic = args.length > 1 ? args[1] : "MQ_RUST_PERSIST";
        int expected = args.length > 2 ? Integer.parseInt(args[2]) : 15;

        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("mq_rust_pull_only");
        consumer.setNamesrvAddr(namesrv);
        consumer.setVipChannelEnabled(false);
        consumer.start();

        Set<MessageQueue> queues = consumer.fetchSubscribeMessageQueues(topic);
        int received = 0;
        for (MessageQueue mq : queues) {
            long max = consumer.maxOffset(mq);
            long min = consumer.minOffset(mq);
            System.out.println("RANGE " + mq + " min=" + min + " max=" + max);
            consumer.updateConsumeOffset(mq, 1);
            long stored = consumer.fetchConsumeOffset(mq, false);
            System.out.println("OFFSET " + mq + " stored=" + stored);
            long offset = 0;
            for (int round = 0; round < 16; round++) {
                PullResult result = consumer.pull(mq, "*", offset, 32);
                if (result.getMsgFoundList() == null || result.getMsgFoundList().isEmpty()) {
                    break;
                }
                for (MessageExt msg : result.getMsgFoundList()) {
                    received++;
                    System.out.println("RECV " + msg.getQueueId() + ":" + msg.getQueueOffset()
                        + " " + new String(msg.getBody(), StandardCharsets.UTF_8));
                }
                offset = result.getNextBeginOffset();
                if (offset >= result.getMaxOffset()) {
                    break;
                }
            }
        }
        consumer.shutdown();
        System.out.println("PULL_ONLY received=" + received + " expected=" + expected);
        if (received < expected) {
            throw new IllegalStateException("restart recovery pull failed");
        }
    }
}
