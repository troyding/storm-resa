package backtype.storm.messaging.netty;

import org.jboss.netty.buffer.ChannelBuffer;

import java.io.DataOutput;

/**
 * Created by Tom.fu on 27/1/2015.
 */
class MetadataMessage {
    public static int MSG_LEN = Long.BYTES + Integer.BYTES;

    public long timeStamp;
    public int totalBytes;

    public MetadataMessage(long timeStamp, int totalBytes) {
        this.timeStamp = timeStamp;
        this.totalBytes = totalBytes;
    }

    ///TODO: Add by Tom, output META_MESSAGE and timestamp information
    void write(DataOutput out) throws Exception {
        out.writeLong(timeStamp);
        out.writeInt(totalBytes);
    }

    static MetadataMessage read(ChannelBuffer buf) {
        return new MetadataMessage(buf.readLong(), buf.readInt());
    }

}
