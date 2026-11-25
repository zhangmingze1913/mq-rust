import org.apache.rocketmq.client.producer.DefaultMQProducer;

/** Start a producer and keep it alive so mqadmin producerConnection can inspect it. */
public class MqRustProducerHold {
    public static void main(String[] args) throws Exception {
        String namesrv = args.length > 0 ? args[0] : "127.0.0.1:9877";
        String group = args.length > 1 ? args[1] : "mq_rust_producer_hold_group";
        int seconds = args.length > 2 ? Integer.parseInt(args[2]) : 15;
        DefaultMQProducer producer = new DefaultMQProducer(group);
        producer.setNamesrvAddr(namesrv);
        producer.setVipChannelEnabled(false);
        producer.start();
        System.out.println("PRODUCER_HOLD started group=" + group);
        Thread.sleep(seconds * 1000L);
        producer.shutdown();
    }
}
