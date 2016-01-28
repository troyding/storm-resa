/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backtype.storm.utils;

import com.google.common.util.concurrent.AtomicDouble;
import com.lmax.disruptor.ClaimStrategy;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.WaitStrategy;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A single consumer queue that uses the LMAX Disruptor. They key to the performance is
 * the ability to catch up to the producer by processing tuples in batches.
 */
public class TracedDisruptorQueue extends DisruptorQueue {

    class QueueState {
        public AtomicLong totalLen = new AtomicLong();
        public AtomicDouble arrIntSum = new AtomicDouble();
        public AtomicDouble arrIntSum2 = new AtomicDouble();

        public void updateQLen(int len) {
            totalLen.addAndGet(len);
        }

        public void updateArriveInterval(double interval) {
            arrIntSum.addAndGet(interval);
            arrIntSum2.addAndGet(interval * interval);
        }

        void getAndReset(Map<String, Number> output) {
            output.put("totalQueueLen", totalLen.getAndSet(0));
            output.put("arrIntervalSum", arrIntSum.getAndSet(0));
            output.put("arrIntervalSum2", arrIntSum2.getAndSet(0));
        }

    }

    private AtomicInteger counter = new AtomicInteger();
    private long last = System.currentTimeMillis();
    private final int sampleInterval;
    private QueueState queueState = new QueueState();
    private AtomicLong lastArriveTime = new AtomicLong(0);

    public TracedDisruptorQueue(ClaimStrategy claim, WaitStrategy wait, Map<String, Object> conf) {
        super(claim, wait);
        float sampleRate = 0.05f;
        if (conf.containsKey("topology.queue.sample.rate")) {
            sampleRate = ((Number) conf.get("topology.queue.sample.rate")).floatValue();
        }
        this.sampleInterval = Math.max((int) (1 / sampleRate), 2);
    }

    public void publish(Object obj, boolean block) throws InsufficientCapacityException {
        super.publish(obj, block);
        if (counter.incrementAndGet() % sampleInterval == 0) {
            queueState.updateQLen((int) population());
            lastArriveTime.set(System.nanoTime());
        } else if (lastArriveTime.get() != 0) {
            long lastArriveTimeTmp = lastArriveTime.getAndSet(0);
            if (lastArriveTimeTmp != 0) {
                queueState.updateArriveInterval((System.nanoTime() - lastArriveTimeTmp) / 1000000.0);
            }
        }
    }

    @Override
    public Object getState() {
        long count = counter.getAndSet(0);
        if (count < sampleInterval) {
            return Collections.emptyMap();
        }
        Map state = new HashMap<String, Number>();
        state.put("totalCount", (int) count);
        queueState.getAndReset(state);
        state.put("sampleCount", count / sampleInterval);
        long now = System.currentTimeMillis();
        state.put("duration", now - last);
        last = now;
        return state;
    }

}
