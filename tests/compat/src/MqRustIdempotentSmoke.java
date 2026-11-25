import java.nio.charset.StandardCharsets;
import java.util.Set;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

/** R3.3 idempotent producer smoke: same UNIQ_KEY twice -> one stored message. */
public class MqRustIdempotentSmoke {
    public static void main(String[] args) throws Exception {
        String namesrv = args.length > 0 ? args[0] : "127.0.0.1:9877";
        String topic = args.length > 1 ? args[1] : "MQ_RUST_IDEMPOTENT";
        String uniq = "00112233445566778899AABBCCDDEEFF";

        DefaultMQProducer producer = new DefaultMQProducer("mq_rust_idem_producer");
        producer.setNamesrvAddr(namesrv);
        producer.setVipChannelEnabled(false);
        producer.start();
        Message msg = new Message(topic, "idem-body".getBytes(StandardCharsets.UTF_8));
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, uniq);
        System.out.println("IDEM SEND 1 " + producer.send(msg));
        System.out.println("IDEM SEND 2 " + producer.send(msg));
        producer.shutdown();

        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("mq_rust_idem_consumer");
        consumer.setNamesrvAddr(namesrv);
        consumer.setVipChannelEnabled(false);
        consumer.start();
        Set<MessageQueue> queues = consumer.fetchSubscribeMessageQueues(topic);
        int count = 0;
        for (MessageQueue mq : queues) {
            PullResult result = consumer.pull(mq, "*", 0, 32);
            if (result.getMsgFoundList() != null) {
                count += result.getMsgFoundList().size();
                for (MessageExt ignored : result.getMsgFoundList()) {
                    System.out.println("IDEM RECV " + new String(ignored.getBody(), StandardCharsets.UTF_8));
                }
            }
        }
        consumer.shutdown();
        System.out.println("IDEM RESULT count=" + count);
        if (count != 1) {
            throw new IllegalStateException("idempotent smoke failed");
        }
    }
}
