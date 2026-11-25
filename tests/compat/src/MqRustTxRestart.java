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

/** Send a half message, restart broker, then commit via admin and pull it. */
public class MqRustTxRestart {
    private static int pullCount(String namesrv, String topic) throws Exception {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(
            "mq_rust_tx_restart_pull_" + System.currentTimeMillis());
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
        System.out.println("TX_RESTART POST " + url + " -> " + conn.getResponseCode());
    }

    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0] : "send";
        String namesrv = args.length > 1 ? args[1] : "127.0.0.1:9877";
        String topic = args.length > 2 ? args[2] : "MQ_RUST_TX_RESTART";
        String txId = args.length > 3 ? args[3] : "";

        if (mode.equals("send")) {
            TransactionMQProducer producer = new TransactionMQProducer(
                "mq_rust_tx_restart_producer_" + System.currentTimeMillis());
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
                new Message(topic, "tx-restart".getBytes(StandardCharsets.UTF_8)), null);
            producer.shutdown();
            System.out.println("TX_RESTART txId=" + result.getTransactionId());
            return;
        }

        post("http://127.0.0.1:9091/tx/" + txId + "/commit");
        int count = pullCount(namesrv, topic);
        System.out.println("TX_RESTART RESULT visible=" + count);
        if (count != 1) {
            throw new IllegalStateException("tx restart commit failed");
        }
    }
}
