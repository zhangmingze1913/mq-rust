import java.nio.charset.StandardCharsets;
import java.util.Set;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

/** R5.1 arbitrary start-deliver timestamp. */
public class MqRustStartDeliverSmoke {
    private static int pullCount(String namesrv, String topic) throws Exception {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(
            "mq_rust_start_pull_" + System.currentTimeMillis());
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
                    System.out.println("START RECV " + new String(m.getBody(), StandardCharsets.UTF_8));
                }
            }
        }
        consumer.shutdown();
        return count;
    }

    public static void main(String[] args) throws Exception {
        String namesrv = args.length > 0 ? args[0] : "127.0.0.1:9877";
        String topic = args.length > 1 ? args[1] : "MQ_RUST_START_DELIVER";

        DefaultMQProducer producer = new DefaultMQProducer("mq_rust_start_producer");
        producer.setNamesrvAddr(namesrv);
        producer.setVipChannelEnabled(false);
        producer.start();
        Thread.sleep(1500);
        Message msg = new Message(topic, "start-3s".getBytes(StandardCharsets.UTF_8));
        long due = System.currentTimeMillis() + 10000;
        MessageAccessor.putProperty(msg, "__STARTDELIVERTIME", String.valueOf(due));
        System.out.println("START SENT at=" + System.currentTimeMillis() + " due=" + due);
        producer.send(msg);
        producer.shutdown();

        int early = pullCount(namesrv, topic);
        System.out.println("START EARLY at=" + System.currentTimeMillis() + " count=" + early);
        Thread.sleep(12000);
        int late = pullCount(namesrv, topic);
        System.out.println("START RESULT early=" + early + " late=" + late);
        if (early != 0 || late != 1) {
            throw new IllegalStateException("start-deliver smoke failed");
        }
    }
}
