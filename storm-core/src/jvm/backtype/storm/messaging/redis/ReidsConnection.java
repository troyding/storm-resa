package backtype.storm.messaging.redis;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.utils.Utils;
import redis.clients.jedis.Jedis;

import java.nio.ByteBuffer;

/**
 * Created by ding on 14-8-2.
 */
class ReidsConnection implements IConnection {

    private byte[] queue;
    private Jedis jedis;

    ReidsConnection(byte[] queue, Jedis jedis) {
        this.queue = queue;
        this.jedis = jedis;
    }

    @Override
    public TaskMessage recv(int flags) {
        byte[] data = null;
        if (flags == 0) {
            while (true) {
                data = jedis.lpop(queue);
                if (data != null) {
                    break;
                } else {
                    Utils.sleep(1);
                }
            }
        } else {
            data = jedis.lpop(queue);
        }
        TaskMessage msg = new TaskMessage(-1, null);
        msg.deserialize(ByteBuffer.wrap(data));
        return msg;
    }

    @Override
    public void send(int taskId, byte[] payload) {
        TaskMessage msg = new TaskMessage(taskId, payload);
        ByteBuffer buf = msg.serialize();
        jedis.rpush(queue, buf.array());
    }

    @Override
    public void close() {
        jedis.disconnect();
    }
}
