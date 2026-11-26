import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;

/** R211: duplicate END_TRANSACTION commit must not duplicate the message. */
public class MqRustTxDupCommit {
    private static int pullCount(String namesrv, String topic) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("dup_" + System.nanoTime());
        consumer.setNamesrvAddr(namesrv);
        consumer.subscribe(topic, "*");
        final int[] count = {0};
        consumer.registerMessageListener((MessageListenerConcurrently) (List<MessageExt> msgs,
                org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext ctx) -> {
            count[0] += msgs.size();
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        Thread.sleep(1500);
        consumer.shutdown();
        return count[0];
    }

    public static void main(String[] args) throws Exception {
        String namesrv = args.length > 0 ? args[0] : "127.0.0.1:9877";
        String topic = args.length > 1 ? args[1] : "MQ_RUST_TX_DUP";
        TransactionMQProducer producer = new TransactionMQProducer("mq_rust_duptx_" + System.nanoTime());
        producer.setNamesrvAddr(namesrv);
        producer.setTransactionListener(new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                return LocalTransactionState.COMMIT_MESSAGE;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });
        producer.start();
        TransactionSendResult result = producer.sendMessageInTransaction(
                new Message(topic, "dup-body".getBytes(StandardCharsets.UTF_8)), null);
        String txId = result.getTransactionId();
        String msgId = result.getMsgId();
        String brokerName = result.getMessageQueue().getBrokerName();
        String addr = producer.getDefaultMQProducerImpl().getMqClientFactory()
                .findBrokerAddressInPublish(brokerName);
        MQClientAPIImpl api = producer.getDefaultMQProducerImpl().getMqClientFactory()
                .getMQClientAPIImpl();
        EndTransactionRequestHeader header = new EndTransactionRequestHeader();
        header.setTransactionId(txId);
        header.setMsgId(msgId);
        header.setCommitOrRollback(8);
        header.setFromTransactionCheck(false);
        header.setProducerGroup(producer.getProducerGroup());
        api.endTransactionOneway(addr, header, null, 3000);
        api.endTransactionOneway(addr, header, null, 3000);
        Thread.sleep(1000);
        producer.shutdown();
        int count = pullCount(namesrv, topic);
        System.out.println("DUP RESULT tx=" + txId + " count=" + count);
        if (count != 1) {
            throw new IllegalStateException("duplicate commit visible: " + count);
        }
    }
}
