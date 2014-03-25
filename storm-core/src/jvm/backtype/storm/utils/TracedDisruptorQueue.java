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

import com.lmax.disruptor.*;
import com.sun.org.apache.bcel.internal.generic.ALOAD;

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
        public AtomicInteger totalLen = new AtomicInteger();

        public void update(int len) {
            count.incrementAndGet();
            totalLen.addAndGet(len);
        }

        void getAndReset(Map<String, Integer> output) {
            output.put("sampleCount", count.getAndSet(0));
            output.put("totalQueueLen", totalLen.getAndSet(0));
        }

    }

    private AtomicInteger conuter = new AtomicInteger();
    private long sampleCounterBase = 0;
    private AtomicLong last = new AtomicLong(System.currentTimeMillis());
    private final int sampleInterval;
    private QueueState queueState = new QueueState();

    public TracedDisruptorQueue(ClaimStrategy claim, WaitStrategy wait, Map<String, Object> conf) {
        super(claim, wait);
        this.sampleInterval = Utils.getInt(conf.getOrDefault("topology.queue.trace.interval", 20));
    }

    public boolean doSample() {
        return (sampleCounterBase + conuter.incrementAndGet()) % sampleInterval == 0;
    }

    public void publish(Object obj, boolean block) throws InsufficientCapacityException {
        super.publish(obj, block);
        conuter.addAndGet(1);
        if (doSample()) {
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
        Map state = new HashMap<String, Integer>();
        state.put("totalCount", (int) count);
        queueState.getAndReset(state);
        return state;
    }

}
