import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * Created by krishna on 10/4/15.
 */
public class NodeGen {

    public Node getNode() {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("http.enabled", "false")
                .put("transport.tcp.port", "9300-9400")
                .put("discovery.zen.ping.multicast.enabled", "false")
                .put("discovery.zen.ping.unicast.hosts", "128.199.195.255").build();
        return nodeBuilder().client(true).settings(settings).clusterName("elasticsearch").node();
    }
}
