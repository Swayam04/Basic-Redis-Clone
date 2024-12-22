package replication;

import commands.RedisCommand;
import core.RedisServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resp.RespEncoder;
import utils.ClientState;

import java.util.ArrayList;
import java.util.List;

public class ReplicationManager {
    private static final List<ClientState> replicaClients = new ArrayList<>();
    private static final Logger log = LoggerFactory.getLogger(ReplicationManager.class);

    public static List<ClientState> getReplicaClients() {
        return replicaClients;
    }

    public static void addReplicaClient(ClientState clientState) {
        replicaClients.add(clientState);
        RedisServer.getReplicationInfo().addConnectedSlaves();
    }

    public static void removeReplicaClient(ClientState clientState) {
        replicaClients.remove(clientState);
        RedisServer.getReplicationInfo().addConnectedSlaves(-1);
    }

    public static void propagateToReplicas(RedisCommand redisCommand) {
        for (ClientState clientState : replicaClients) {
            String response = RespEncoder.encodeCommand(redisCommand);
            log.info("propagating to replicas: {}", response);
            clientState.responseQueue().offer(response);
        }
    }
}
