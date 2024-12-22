package replication;

import commands.RedisCommand;
import core.RedisServer;
import resp.RespEncoder;
import utils.ClientState;
import utils.ParsedCommand;

import java.util.ArrayList;
import java.util.List;

public class ReplicationManager {
    private static final List<ClientState> replicaClients = new ArrayList<>();

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
            clientState.responseQueue().offer(RespEncoder.encodeCommand(redisCommand));
        }
    }
}
