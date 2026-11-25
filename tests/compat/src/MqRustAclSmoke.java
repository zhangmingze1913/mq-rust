import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

public class MqRustAclSmoke {
    public static void main(String[] args) throws Exception {
        String namesrv = args.length > 0 ? args[0] : "127.0.0.1:9877";
        String topic = args.length > 1 ? args[1] : "MQ_ACL";
        DefaultMQProducer producer = new DefaultMQProducer(
                "mq_rust_acl_producer",
                new AclClientRPCHook(new SessionCredentials("mykey", "secret")));
        producer.setNamesrvAddr(namesrv);
        producer.start();
        try {
            Message msg = new Message(topic, "acl-body".getBytes());
            SendResult result = producer.send(msg);
            System.out.println("ACL RESULT status=" + result.getSendStatus()
                    + " msgId=" + result.getMsgId());
        } finally {
            producer.shutdown();
        }
    }
}
