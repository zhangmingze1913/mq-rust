import java.nio.charset.StandardCharsets;
import java.util.Set;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

/** R4 delay smoke: level 2 (5s) is invisible early, visible after due. */
public class MqRustDelaySmoke {
    private static int pullCount(String namesrv, String topic) throws Exception {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(
            "mq_rust_delay_pull_" + System.currentTimeMillis());
        consumer.setNamesrvAddr(namesrv);
        consumer.setVipChannelEnabled(false);
        consumer.start();
        int count = 0;
        Set<MessageQueue> queues = consumer.fetchSubscribeMessageQueues(topic);
        for (MessageQueue mq : queues) {
            PullResult result = consumer.pull(mq, "*", 0, 32);
            if (result.getMsgFoundList() != null) {
                count += result.getMsgFoundList().size();
                for (MessageExt m : result.getMsgFoundList()) {
                    System.out.println("DELAY RECV " + new String(m.getBody(), StandardCharsets.UTF_8));
                }
            }
        }
        consumer.shutdown();
        return count;
    }

    public static void main(String[] args) throws Exception {
        String namesrv = args.length > 0 ? args[0] : "127.0.0.1:9877";
        String topic = args.length > 1 ? args[1] : "MQ_RUST_DELAY";

        DefaultMQProducer producer = new DefaultMQProducer("mq_rust_delay_producer");
        producer.setNamesrvAddr(namesrv);
        producer.setVipChannelEnabled(false);
        producer.start();
        Thread.sleep(2000);
        Message msg = new Message(topic, "delay-10s".getBytes(StandardCharsets.UTF_8));
        msg.setDelayTimeLevel(3);
        producer.send(msg);
        producer.shutdown();

        int early = pullCount(namesrv, topic);
        System.out.println("DELAY EARLY count=" + early);
        Thread.sleep(11000);
        int late = pullCount(namesrv, topic);
        System.out.println("DELAY LATE count=" + late);
        if (early != 0 || late != 1) {
            throw new IllegalStateException("delay smoke failed");
        }
    }
}
