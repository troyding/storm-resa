package backtype.storm.scheduler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * Created by ding on 14-8-1.
 */
public class Util {
    public static Map<ExecutorDetails, String> computeExecutors(GeneralTopologyDetails topoDetails,
                                                                List<ComponentDetails> compDetails) {
        Map<ExecutorDetails, String> ret = new HashMap<>();
        compDetails.forEach(c -> {
            int numExecutors = c.getNumExecutors();
            int[] tasks = c.getTasks();
            String compId = c.getComponentId();
            if (numExecutors >= tasks.length) {
                IntStream.of(tasks).mapToObj(t -> new ExecutorDetails(t, t)).forEach(e -> ret.put(e, compId));
            } else {
                int avgCnt = tasks.length / numExecutors;
                int addSize = tasks.length % numExecutors;
                int k = 0;
                for (int i = 0; i < numExecutors; i++) {
                    int start = tasks[k];
                    k += avgCnt;
                    if (i < addSize) {
                        k++;
                    }
                    ret.put(new ExecutorDetails(start, tasks[k - 1]), c.getComponentId());
                }
            }
        });
        return ret;
    }
}
