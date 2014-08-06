package backtype.storm.scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by ding on 14-8-1.
 */
public class Util {
    public static Map<ExecutorDetails, String> computeExecutors(List<ComponentDetails> compDetails) {
        Map<ExecutorDetails, String> ret = new HashMap<>();
        compDetails.forEach(c -> computeExecutors(c).forEach(e -> ret.put(e, c.getComponentId())));
        return ret;
    }

    public static List<ExecutorDetails> computeExecutors(ComponentDetails compoDetails) {
        int numExecutors = compoDetails.getNumExecutors();
        int[] tasks = compoDetails.getTasks();
        List<ExecutorDetails> executors;
        if (numExecutors >= tasks.length) {
            executors = IntStream.of(tasks).mapToObj(t -> new ExecutorDetails(t, t)).collect(Collectors.toList());
        } else {
            int avgCnt = tasks.length / numExecutors;
            int addSize = tasks.length % numExecutors;
            int k = 0;
            executors = new ArrayList<>(numExecutors);
            for (int i = 0; i < numExecutors; i++) {
                int start = tasks[k];
                k += avgCnt;
                if (i < addSize) {
                    k++;
                }
                executors.add(new ExecutorDetails(start, tasks[k - 1]));
            }
        }
        return executors;
    }
}
