package backtype.storm.scheduler;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;

import java.util.Map;

/**
 * Created by ding on 14-7-13.
 */
public class GeneralTopologyDetails {
    String topologyId;
    Map topologyConf;
    StormTopology topology;

    public GeneralTopologyDetails(Map topologyConf, StormTopology topology, String topologyId) {
        this.topologyConf = topologyConf;
        this.topology = topology;
        this.topologyId = topologyId;
    }

    public String getId() {
        return topologyId;
    }

    public String getName() {
        return (String) this.topologyConf.get(Config.TOPOLOGY_NAME);
    }

    public Map getConf() {
        return topologyConf;
    }

    public StormTopology getTopology() {
        return topology;
    }
}
