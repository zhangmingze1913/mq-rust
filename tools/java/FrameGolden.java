import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * Prints one RocketMQ 4.9.8 Remoting frame as hex so MQ-rust can use it as a
 * cross-language golden test.
 */
public class FrameGolden {
    public static void main(String[] args) throws Exception {
        RemotingCommand cmd = RemotingCommand.createRequestCommand(10, null);
        cmd.setOpaque(42);
        cmd.setBody(new byte[] {1, 2, 3});
        java.nio.ByteBuffer buf = cmd.encode();
        byte[] arr = new byte[buf.remaining()];
        buf.get(arr);
        StringBuilder sb = new StringBuilder(arr.length * 2);
        for (byte b : arr) {
            sb.append(String.format("%02x", b));
        }
        System.out.println(sb);
    }
}
