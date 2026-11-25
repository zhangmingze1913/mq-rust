import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
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

/** R3 admin: manually commit a pending half transaction via HTTP. */
public class MqRustTxManualSmoke {
    private static int pullCount(String namesrv, String topic) throws Exception {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(
            "mq_rust_manual_pull_" + System.currentTimeMillis());
        consumer.setNamesrvAddr(namesrv);
        consumer.setVipChannelEnabled(false);
        consumer.start();
        int count = 0;
        Set<MessageQueue> queues = consumer.fetchSubscribeMessageQueues(topic);
        for (MessageQueue mq : queues) {
            PullResult result = consumer.pull(mq, "*", 0, 32);
            if (result.getMsgFoundList() != null) {
                count += result.getMsgFoundList().size();
            }
        }
        consumer.shutdown();
        return count;
    }

    private static void post(String url) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        try (OutputStream os = conn.getOutputStream()) {
            os.write(new byte[0]);
        }
        System.out.println("MANUAL POST " + url + " -> " + conn.getResponseCode());
    }

    public static void main(String[] args) throws Exception {
        String namesrv = args.length > 0 ? args[0] : "127.0.0.1:9877";
        String topic = args.length > 1 ? args[1] : "MQ_RUST_TX_MANUAL";
        String admin = args.length > 2 ? args[2] : "http://127.0.0.1:9091";

        TransactionMQProducer producer = new TransactionMQProducer(
            "mq_rust_manual_producer_" + System.currentTimeMillis());
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
        TransactionSendResult result = producer.sendMessageInTransaction(
            new Message(topic, "manual-commit".getBytes(StandardCharsets.UTF_8)), null);
        producer.shutdown();
        String txId = result.getTransactionId();
        System.out.println("MANUAL txId=" + txId);

        post(admin + "/tx/" + txId + "/commit");
        int count = pullCount(namesrv, topic);
        System.out.println("MANUAL RESULT visible=" + count);
        if (count != 1) {
            throw new IllegalStateException("manual commit failed");
        }
    }
}
