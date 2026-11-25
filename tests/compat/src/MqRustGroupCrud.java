import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

/** Exercise subscription group create/list/delete (200/201/207). */
public class MqRustGroupCrud {
    public static void main(String[] args) throws Exception {
        String broker = args.length > 0 ? args[0] : "127.0.0.1:10921";
        String group = args.length > 1 ? args[1] : "mq_rust_crud_group";
        DefaultMQAdminExt admin = new DefaultMQAdminExt("mq_rust_crud_" + System.currentTimeMillis());
        admin.setVipChannelEnabled(false);
        admin.start();

        SubscriptionGroupConfig config = new SubscriptionGroupConfig();
        config.setGroupName(group);
        admin.createAndUpdateSubscriptionGroupConfig(broker, config);
        SubscriptionGroupWrapper afterCreate = admin.getAllSubscriptionGroup(broker, 3000);
        System.out.println("GROUP_CRUD afterCreate=" + afterCreate.getSubscriptionGroupTable().containsKey(group));

        admin.deleteSubscriptionGroup(broker, group, false);
        SubscriptionGroupWrapper afterDelete = admin.getAllSubscriptionGroup(broker, 3000);
        System.out.println("GROUP_CRUD afterDelete=" + afterDelete.getSubscriptionGroupTable().containsKey(group));

        admin.shutdown();
    }
}
