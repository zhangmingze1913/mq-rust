import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;

/** Consume over TLS. */
public class MqRustTlsConsumer {
    public static void main(String[] args) throws Exception {
        String namesrv = args.length > 0 ? args[0] : "127.0.0.1:9877";
        String topic = args.length > 1 ? args[1] : "MQ_TLS";
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("mq_rust_tls_consumer");
        consumer.setNamesrvAddr(namesrv);
        consumer.setVipChannelEnabled(false);
        consumer.setUseTLS(true);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(topic, "*");
        final int[] count = {0};
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            count[0] += msgs.size();
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        Thread.sleep(5000);
        consumer.shutdown();
        System.out.println("TLS CONSUME count=" + count[0]);
        if (count[0] < 1) {
            throw new IllegalStateException("no message over TLS");
        }
    }
}
