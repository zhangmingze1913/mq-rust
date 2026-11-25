import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

/** R3.2 check-back smoke: executeLocal=UNKNOW, checkLocal=COMMIT. */
public class MqRustTxCheckBackSmoke {
    private static int pullCount(String namesrv, String topic) throws Exception {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(
            "mq_rust_tx_check_pull_" + System.currentTimeMillis());
        consumer.setNamesrvAddr(namesrv);
        consumer.setVipChannelEnabled(false);
        consumer.start();
        int count = 0;
        Set<MessageQueue> queues = consumer.fetchSubscribeMessageQueues(topic);
        for (MessageQueue mq : queues) {
            long offset = 0;
            PullResult result = consumer.pull(mq, "*", offset, 32);
            if (result.getMsgFoundList() != null) {
                for (MessageExt ignored : result.getMsgFoundList()) {
                    count++;
                }
            }
        }
        consumer.shutdown();
        return count;
    }

    public static void main(String[] args) throws Exception {
        String namesrv = args.length > 0 ? args[0] : "127.0.0.1:9877";
        String topic = args.length > 1 ? args[1] : "MQ_RUST_TX_CHECK";
        AtomicInteger checks = new AtomicInteger();

        TransactionMQProducer producer = new TransactionMQProducer(
            "mq_rust_tx_check_producer_" + System.currentTimeMillis());
        producer.setNamesrvAddr(namesrv);
        producer.setVipChannelEnabled(false);
        producer.setTransactionListener(new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                return LocalTransactionState.UNKNOW;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                checks.incrementAndGet();
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });
        producer.start();
        producer.sendMessageInTransaction(
            new Message(topic, "check-body".getBytes(StandardCharsets.UTF_8)), null);
        System.out.println("TX CHECK waiting for check-back...");
        Thread.sleep(8000);
        producer.shutdown();

        int count = pullCount(namesrv, topic);
        System.out.println("TX CHECK RESULT checks=" + checks.get() + " visible=" + count);
        if (checks.get() < 1 || count != 1) {
            throw new IllegalStateException("check-back smoke failed");
        }
    }
}
