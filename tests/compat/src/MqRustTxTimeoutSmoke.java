import java.nio.charset.StandardCharsets;
import java.util.Set;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

/** R3.1 timeout smoke: Half -> UNKNOW -> broker expires -> consumer sees 0. */
public class MqRustTxTimeoutSmoke {
    private static int pullCount(String namesrv, String topic) throws Exception {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(
            "mq_rust_tx_timeout_pull_" + System.currentTimeMillis());
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
        String topic = args.length > 1 ? args[1] : "MQ_RUST_TX_TIMEOUT";

        TransactionMQProducer producer = new TransactionMQProducer(
            "mq_rust_tx_timeout_producer_" + System.currentTimeMillis());
        producer.setNamesrvAddr(namesrv);
        producer.setVipChannelEnabled(false);
        producer.setTransactionListener(new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                return LocalTransactionState.UNKNOW;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                return LocalTransactionState.UNKNOW;
            }
        });
        producer.start();
        org.apache.rocketmq.client.producer.TransactionSendResult sendResult =
            producer.sendMessageInTransaction(
                new Message(topic, "timeout-body".getBytes(StandardCharsets.UTF_8)), null);
        System.out.println("TX TIMEOUT msgId=" + sendResult.getMsgId());
        producer.shutdown();

        System.out.println("TX TIMEOUT waiting for broker expiry...");
        Thread.sleep(13000);
        int count = pullCount(namesrv, topic);
        System.out.println("TX TIMEOUT RESULT visible=" + count);
        if (count != 0) {
            throw new IllegalStateException("half message became visible");
        }
    }
}
