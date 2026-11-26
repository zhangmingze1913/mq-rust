import java.nio.charset.StandardCharsets;
import java.util.Set;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

/** R2 SQL92 property filter: price > 100. */
public class MqRustSql92Smoke {
    public static void main(String[] args) throws Exception {
        String namesrv = args.length > 0 ? args[0] : "127.0.0.1:9877";
        String topic = args.length > 1 ? args[1] : "MQ_RUST_SQL92";

        DefaultMQProducer producer = new DefaultMQProducer("mq_rust_sql92_producer");
        producer.setNamesrvAddr(namesrv);
        producer.setVipChannelEnabled(false);
        producer.start();
        Message cheap = new Message(topic, "cheap".getBytes(StandardCharsets.UTF_8));
        cheap.putUserProperty("price", "50");
        cheap.putUserProperty("region", "CN");
        Message expensive = new Message(topic, "expensive".getBytes(StandardCharsets.UTF_8));
        expensive.putUserProperty("price", "150");
        expensive.putUserProperty("region", "CN");
        producer.send(cheap);
        producer.send(expensive);
        producer.shutdown();

        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("mq_rust_sql92_consumer");
        consumer.setNamesrvAddr(namesrv);
        consumer.setVipChannelEnabled(false);
        consumer.start();
        // PullAPIWrapper checks brokerVersionTable filled by heartbeat; wait for the first one.
        Thread.sleep(3000);
        Set<MessageQueue> queues = consumer.fetchSubscribeMessageQueues(topic);
        int count = 0;
        for (MessageQueue mq : queues) {
            PullResult result = consumer.pull(mq, MessageSelector.bySql("price > 100"), 0, 32);
            if (result.getMsgFoundList() != null) {
                for (MessageExt msg : result.getMsgFoundList()) {
                    count++;
                    System.out.println("SQL92 RECV " + new String(msg.getBody(), StandardCharsets.UTF_8)
                        + " price=" + msg.getUserProperty("price"));
                }
            }
        }
        consumer.shutdown();
        System.out.println("SQL92 RESULT count=" + count);
        if (count != 1) {
            throw new IllegalStateException("sql92 smoke failed");
        }
    }
}
