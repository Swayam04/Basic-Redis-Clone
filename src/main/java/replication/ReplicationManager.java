package replication;

import commands.RedisCommand;
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
    }

    public static void removeReplicaClient(ClientState clientState) {
        replicaClients.remove(clientState);
    }

    public static void propagateToReplicas(RedisCommand redisCommand) {
        for (ClientState clientState : replicaClients) {
            clientState.responseQueue().offer(RespEncoder.encodeCommand(redisCommand));
        }
    }
}
