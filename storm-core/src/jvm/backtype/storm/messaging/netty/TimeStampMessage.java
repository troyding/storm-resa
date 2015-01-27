package backtype.storm.messaging.netty;

/**
 * Created by Tom.fu on 27/1/2015.
 */
public class TimeStampMessage {
    public long timeStamp;
    public ControlMessage controlMessage;
    public int totalBytes;

    public TimeStampMessage(long timeStamp, int totalBytes){
        this.timeStamp = timeStamp;
        this.totalBytes = totalBytes;
        this.controlMessage = ControlMessage.TS_MESSAGE;
    }

    static int payLoadLen = Long.BYTES + Integer.BYTES;
}
