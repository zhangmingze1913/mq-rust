import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/** Send over RocketMQ TLS (server: self-signed cert). */
public class MqRustTlsSmoke {
    public static void main(String[] args) throws Exception {
        String namesrv = args.length > 0 ? args[0] : "127.0.0.1:9877";
        String topic = args.length > 1 ? args[1] : "MQ_TLS";
        DefaultMQProducer producer = new DefaultMQProducer("mq_rust_tls_producer");
        producer.setNamesrvAddr(namesrv);
        producer.setVipChannelEnabled(false);
        producer.setUseTLS(true);
        producer.start();
        SendResult result = producer.send(new Message(topic, "hello-tls".getBytes("UTF-8")));
        System.out.println("TLS RESULT status=" + result.getSendStatus() + " msgId=" + result.getMsgId());
        producer.shutdown();
    }
}
