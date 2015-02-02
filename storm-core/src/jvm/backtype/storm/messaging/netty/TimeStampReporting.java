package backtype.storm.messaging.netty;

import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Tom.fu on 27/1/2015.
 */
class TimeStampReporting extends Thread {

    private static class Counter {
        int totalCount;
        long totalDelayMs;
        long totalBytes;

        public synchronized void delayUpdate(long delay, int bytes) {
            totalCount++;
            totalBytes += bytes;
            totalDelayMs += delay;
        }

    }

    private static final Logger LOG = LoggerFactory.getLogger(StormServerHandler.class);
    private long reportIntervalMs = 0;

    private Map<SocketAddress, Counter> host2Delay = new ConcurrentHashMap<>();

    public TimeStampReporting(long reportIntervalMs) {
        this.reportIntervalMs = Math.max(1000, reportIntervalMs);
        setDaemon(true);
        start();
    }

    public void update(SocketAddress host, long delay, int bytes) {
        host2Delay.computeIfAbsent(host, key -> new Counter()).delayUpdate(delay, bytes);
    }

    @Override
    public void run() {
        while (true) {
            Utils.sleep(reportIntervalMs);
            Long curr = System.currentTimeMillis();
            Iterator<Map.Entry<SocketAddress, Counter>> iterator = host2Delay.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<SocketAddress, Counter> e = iterator.next();
                iterator.remove();
                Counter counter = e.getValue();
                LOG.info("Reporting at: " + curr + ", host: " + e.getKey() + ", tCnt: " + counter.totalCount +
                        ", tDelayMs: " + counter.totalDelayMs + ", tBytes: " + counter.totalBytes);
            }
        }
    }
}
