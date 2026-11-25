import java.nio.charset.StandardCharsets;
import java.util.Set;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

/** R3 transaction smoke: one commit, one rollback, verify only commit is visible. */
public class MqRustTxSmoke {
    private static int pullCount(String namesrv, String topic) throws Exception {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(
            "mq_rust_tx_pull_" + System.currentTimeMillis());
        consumer.setNamesrvAddr(namesrv);
        consumer.setVipChannelEnabled(false);
        consumer.start();
        int count = 0;
        Set<MessageQueue> queues = consumer.fetchSubscribeMessageQueues(topic);
        for (MessageQueue mq : queues) {
            long offset = 0;
            for (int round = 0; round < 8; round++) {
                PullResult result = consumer.pull(mq, "*", offset, 32);
                if (result.getMsgFoundList() == null || result.getMsgFoundList().isEmpty()) {
                    break;
                }
                for (MessageExt msg : result.getMsgFoundList()) {
                    count++;
                    System.out.println("TX PULL " + topic + " body="
                        + new String(msg.getBody(), StandardCharsets.UTF_8));
                }
                offset = result.getNextBeginOffset();
                if (offset >= result.getMaxOffset()) {
                    break;
                }
            }
        }
        consumer.shutdown();
        return count;
    }

    public static void main(String[] args) throws Exception {
        String namesrv = args.length > 0 ? args[0] : "127.0.0.1:9877";
        String commitTopic = args.length > 1 ? args[1] : "MQ_RUST_TX_COMMIT";
        String rollbackTopic = args.length > 2 ? args[2] : "MQ_RUST_TX_ROLLBACK";

        TransactionMQProducer producer = new TransactionMQProducer(
            "mq_rust_tx_producer_" + System.currentTimeMillis());
        producer.setNamesrvAddr(namesrv);
        producer.setVipChannelEnabled(false);
        producer.setTransactionListener(new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                return new String(msg.getBody(), StandardCharsets.UTF_8).startsWith("commit")
                    ? LocalTransactionState.COMMIT_MESSAGE
                    : LocalTransactionState.ROLLBACK_MESSAGE;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });
        producer.start();

        TransactionSendResult commit = producer.sendMessageInTransaction(
            new Message(commitTopic, "commit-body".getBytes(StandardCharsets.UTF_8)), null);
        TransactionSendResult rollback = producer.sendMessageInTransaction(
            new Message(rollbackTopic, "rollback-body".getBytes(StandardCharsets.UTF_8)), null);
        System.out.println("TX SEND commit=" + commit.getLocalTransactionState()
            + " txId=" + commit.getTransactionId());
        System.out.println("TX SEND rollback=" + rollback.getLocalTransactionState()
            + " txId=" + rollback.getTransactionId());
        Thread.sleep(1000);
        producer.shutdown();

        int commitCount = pullCount(namesrv, commitTopic);
        int rollbackCount = pullCount(namesrv, rollbackTopic);
        System.out.println("TX RESULT commit=" + commitCount + " rollback=" + rollbackCount);
        if (commitCount != 1 || rollbackCount != 0) {
            throw new IllegalStateException("transaction smoke failed");
        }
    }
}
