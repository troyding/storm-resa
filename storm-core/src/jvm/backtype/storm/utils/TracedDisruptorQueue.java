/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backtype.storm.utils;

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
        public AtomicInteger count = new AtomicInteger();
        public AtomicLong totalLen = new AtomicLong();

        public void update(int len) {
            count.incrementAndGet();
            totalLen.addAndGet(len);
        }

        void getAndReset(Map<String, Number> output) {
            output.put("sampleCount", count.getAndSet(0));
            output.put("totalQueueLen", totalLen.getAndSet(0));
        }

    }

    private AtomicInteger conuter = new AtomicInteger();
    private long sampleCounterBase = 0;
    private long last = System.currentTimeMillis();
    private final int sampleInterval;
    private QueueState queueState = new QueueState();

    public TracedDisruptorQueue(ClaimStrategy claim, WaitStrategy wait, Map<String, Object> conf) {
        super(claim, wait);
        this.sampleInterval = (int) (1 / ((Number) conf.getOrDefault("topology.queue.sample.rate", 0.05f)).floatValue());
    }

    public void publish(Object obj, boolean block) throws InsufficientCapacityException {
        super.publish(obj, block);
        if ((sampleCounterBase + conuter.incrementAndGet()) % sampleInterval == 0) {
            queueState.update((int) population());
        }
    }

    @Override
    public Object getState() {
        long count = conuter.getAndSet(0);
        if (count == 0) {
            return Collections.emptyMap();
        }
        sampleCounterBase = sampleCounterBase + count;
        Map state = new HashMap<String, Number>();
        state.put("totalCount", (int) count);
        queueState.getAndReset(state);
        long now = System.currentTimeMillis();
        state.put("duration", now - last);
        last = now;
        return state;
    }

}
