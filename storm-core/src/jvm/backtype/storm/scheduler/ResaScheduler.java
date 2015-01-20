package backtype.storm.scheduler;

import backtype.storm.utils.Utils;
import com.netflix.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by ding on 14-8-5.
 */
public class ResaScheduler implements IScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(ResaScheduler.class);

    private CuratorFramework zk;
    private String rootNode;
    private IScheduler defaultScheduler;

    @Override
    public void prepare(Map conf) {
        zk = Utils.newCuratorStarted(conf, (List<String>) conf.get("storm.zookeeper.servers"),
                conf.get("storm.zookeeper.port"));
        rootNode = (String) conf.getOrDefault("storm.scheduler.zk.root", "/resa");
        defaultScheduler = (IScheduler) Utils.newInstance("backtype.storm.scheduler.EvenScheduler");
        LOG.info("Load ResaScheduler successfully, root zk node is {}", rootNode);
    }

    @Override
    public Map<ExecutorDetails, String> computeExecutors(GeneralTopologyDetails topoDetails,
                                                         List<ComponentDetails> compDetails) {
        Map<String, Object> assignment = readAssignment(topoDetails.getId());
        Map<ExecutorDetails, String> ret;
        if (assignment != null) {
            ret = new HashMap<>();
            Map<String, String> exe2comp = (Map<String, String>) assignment.getOrDefault("executors",
                    Collections.EMPTY_MAP);
            exe2comp.forEach((k, v) -> ret.put(convert2ExecutorDetails(k), v));
            // for remain component, using default alg
            Set<String> assignedComp = new HashSet<>(ret.values());
            compDetails.stream().filter(c -> !assignedComp.contains(c.getComponentId()))
                    .forEach(c -> Util.computeExecutors(c).forEach(e -> ret.put(e, c.getComponentId())));
        } else {
            ret = IScheduler.super.computeExecutors(topoDetails, compDetails);
        }
        return ret;
    }

    private Map<String, Object> readAssignment(String topoId) {
        Map<String, Object> assignment = null;
        try {
            byte[] data = zk.getData().forPath(String.format("%s/%s", rootNode, topoId));
            if (data != null && data.length > 0) {
                assignment = (Map<String, Object>) Utils.deserialize(data);
            }
        } catch (Exception e) {
        }
        return assignment;
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        cluster.needsSchedulingTopologies(topologies).forEach(t -> {
            Map<String, Object> assignment = readAssignment(t.getId());
            if (assignment == null) {
                return;
            }
            if (cluster.getAssignmentById(t.getId()) != null) {
                // remove all curr slot
                cluster.freeSlots(cluster.getAssignmentById(t.getId()).getSlots());
            }
            Map<WorkerSlot, List<ExecutorDetails>> workSlot2Executors = new HashMap<>();
            // parse out existing assignment
            ((Map<String, String>) assignment.getOrDefault("assignment", Collections.EMPTY_MAP)).forEach((k, v) -> {
                WorkerSlot slot = convert2WorkerSlot(v, cluster);
                if (slot != null) {
                    workSlot2Executors.computeIfAbsent(slot, s -> new ArrayList<>()).add(convert2ExecutorDetails(k));
                } else {
                    LOG.warn("Cannot find slot: {}", v);
                }
            });
            // get available slot, random select
            List<WorkerSlot> availableSlots = cluster.getAssignableSlots();
            Collections.shuffle(availableSlots);
            for (int i = 0; i < availableSlots.size() && workSlot2Executors.size() < t.getNumWorkers(); i++) {
                workSlot2Executors.computeIfAbsent(availableSlots.get(i), k -> new ArrayList<>());
            }
            // get unassigned executors
            Set<ExecutorDetails> executors = new HashSet<>(t.getExecutors());
            workSlot2Executors.values().forEach(executors::removeAll);
            List<ExecutorDetails>[] tmp = workSlot2Executors.values().stream().toArray(List[]::new);
            int[] pack = packAvg(t.getExecutors().size(), tmp.length);
            List<ExecutorDetails> executorList = new ArrayList<>(executors);
//            Collections.shuffle(executorList);
            int pos = 0;
            for (int i = 0; i < pack.length; i++) {
                List<ExecutorDetails> exeList = tmp[i];
                int cnt = Math.max(0, pack[i] - exeList.size());
                for (int j = 0; j < cnt && pos < executorList.size(); j++) {
                    exeList.add(executorList.get(pos++));
                }
            }
            LOG.info("New assignment: {}", workSlot2Executors);
            // finish assignment
            workSlot2Executors.forEach((slot, eList) -> cluster.assign(slot, t.getId(), eList));
        });
        defaultScheduler.schedule(topologies, cluster);
    }

    private static int[] packAvg(int eleCount, int packCount) {
        int[] ret = new int[packCount];
        Arrays.fill(ret, eleCount / packCount);
        int k = eleCount % packCount;
        for (int i = 0; i < k; i++) {
            ret[i]++;
        }
        return ret;
    }

    private ExecutorDetails convert2ExecutorDetails(String s) {
        String[] exeTmp = s.split("-");
        return new ExecutorDetails(Integer.parseInt(exeTmp[0]), Integer.parseInt(exeTmp[1]));
    }

    private WorkerSlot convert2WorkerSlot(String hostPort, Cluster cluster) {
        String[] workerTmp = hostPort.split(":");
        Integer port = Integer.valueOf(workerTmp[1]);
        Optional<SupervisorDetails> supervisor = cluster.getSupervisorsByHost(workerTmp[0]).stream()
                .filter(s -> s.getAllPorts().contains(port)).findFirst();
        return supervisor.isPresent() ? new WorkerSlot(supervisor.get().getId(), port) : null;
    }
}
