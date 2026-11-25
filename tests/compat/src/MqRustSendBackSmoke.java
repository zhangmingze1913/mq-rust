import java.nio.charset.StandardCharsets;
import java.util.Set;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

/** Isolate CONSUMER_SEND_MSG_BACK (code 36) from the pull API. */
public class MqRustSendBackSmoke {
    public static void main(String[] args) throws Exception {
        String namesrv = args.length > 0 ? args[0] : "127.0.0.1:9877";
        String topic = args.length > 1 ? args[1] : "MQ_RUST_SENDBACK";
        String group = "mq_rust_sendback_group";

        DefaultMQProducer producer = new DefaultMQProducer("mq_rust_sendback_producer");
        producer.setNamesrvAddr(namesrv);
        producer.setVipChannelEnabled(false);
        producer.start();
        producer.send(new Message(topic, "sendback-body".getBytes(StandardCharsets.UTF_8)));
        producer.shutdown();

        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(group);
        consumer.setNamesrvAddr(namesrv);
        consumer.setVipChannelEnabled(false);
        consumer.start();
        Set<MessageQueue> queues = consumer.fetchSubscribeMessageQueues(topic);
        MessageExt target = null;
        for (MessageQueue mq : queues) {
            PullResult result = consumer.pull(mq, "*", 0, 16);
            if (result.getMsgFoundList() != null && !result.getMsgFoundList().isEmpty()) {
                target = result.getMsgFoundList().get(0);
                break;
            }
        }
        if (target == null) {
            throw new IllegalStateException("no message pulled");
        }
        System.out.println("SENDBACK storeHost=" + target.getStoreHost()
            + " brokerName=" + target.getBrokerName()
            + " reconsume=" + target.getReconsumeTimes());
        consumer.sendMessageBack(target, 0);
        System.out.println("SENDBACK request sent");
        Thread.sleep(1000);
        consumer.shutdown();
    }
}
