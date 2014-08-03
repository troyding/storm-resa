package backtype.storm.scheduler;

import java.util.Collection;

/**
 * Created by ding on 14-7-13.
 */
public class ComponentDetails {
    private String componentId;
    private int numExecutors;
    private int[] tasks;

    public ComponentDetails(String componentId, int numExecutors, Collection<Integer> tasks) {
        this.componentId = componentId;
        this.numExecutors = numExecutors;
        this.tasks = tasks.stream().mapToInt(i -> i).sorted().toArray();
    }

    public String getComponentId() {
        return componentId;
    }

    public int getNumExecutors() {
        return numExecutors;
    }

    public int[] getTasks() {
        return tasks;
    }
}
