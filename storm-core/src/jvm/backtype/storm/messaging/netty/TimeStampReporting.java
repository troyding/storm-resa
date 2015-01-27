package backtype.storm.messaging.netty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Tom.fu on 27/1/2015.
 */
public class TimeStampReporting  implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(StormServerHandler.class);
    private long reportIntervalMs = 0;

    private AtomicInteger totalCount;
    private AtomicLong totalDelayMs;
    private AtomicLong totalBytes;

    public TimeStampReporting(long reportIntervalMs){
        reportIntervalMs = Math.max(1000, reportIntervalMs);
        totalBytes.getAndSet(0);
        totalCount.getAndSet(0);
        totalDelayMs.getAndSet(0);
    }

    public void dataUpdate(long delay, int bytes){
        totalCount.getAndIncrement();
        totalDelayMs.getAndAdd(delay);
        totalBytes.getAndAdd(bytes);
    }

    @Override
    public void run() {
        while(true){
            LOG.info("Reporting at: " + System.currentTimeMillis() + ", tCnt: " + totalCount.getAndSet(0)
                    + ", tDelayMs: " + totalDelayMs.getAndSet(0) + ", tBytes: " + totalBytes.getAndSet(0));
            try {
                Thread.sleep(reportIntervalMs);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
