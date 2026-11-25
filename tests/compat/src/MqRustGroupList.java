import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

/** Exercise GET_ALL_SUBSCRIPTIONGROUP_CONFIG (201) directly. */
public class MqRustGroupList {
    public static void main(String[] args) throws Exception {
        String broker = args.length > 0 ? args[0] : "127.0.0.1:10921";
        DefaultMQAdminExt admin = new DefaultMQAdminExt("mq_rust_group_list_" + System.currentTimeMillis());
        admin.setVipChannelEnabled(false);
        admin.start();
        SubscriptionGroupWrapper wrapper = admin.getAllSubscriptionGroup(broker, 3000);
        System.out.println("GROUP_LIST count=" + wrapper.getSubscriptionGroupTable().size());
        for (String group : wrapper.getSubscriptionGroupTable().keySet()) {
            System.out.println("GROUP " + group);
        }
        admin.shutdown();
    }
}
