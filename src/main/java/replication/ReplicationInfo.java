package replication;

import core.RedisServer;

import java.security.SecureRandom;

public class ReplicationInfo {
    private String role;
    private int connectedSlaves = 0;
    private String masterReplId;
    private int masterReplOffset = 0;
    private int secondReplOffset = -1;

    private final SecureRandom random = new SecureRandom();

    public ReplicationInfo() {
        setRole();
        generateReplId();
    }

    private void generateReplId() {
        String ID_CHARACTERS = "abcdefghijklmnopqrstuvwxyz0123456789";
        int ID_LENGTH = 40;
        StringBuilder id = new StringBuilder(ID_LENGTH);
        for (int i = 0; i < ID_LENGTH; i++) {
            int index = random.nextInt(ID_CHARACTERS.length());
            id.append(ID_CHARACTERS.charAt(index));
        }
        masterReplId = id.toString();
    }

    private void setRole() {
        if(RedisServer.currentConfig().properties().containsKey("replicaof")) {
            role = "slave";
            return;
        }
        role = "master";
    }

    public String getRole() {
        return role;
    }

    public String getMasterReplId() {
        return masterReplId;
    }

    public int getConnectedSlavesCount() {
        return connectedSlaves;
    }

    public int getMasterReplOffset() {
        return masterReplOffset;
    }

    public int getSecondReplOffset() {
        return secondReplOffset;
    }

    public void addConnectedSlaves(int... count) {
        if(count.length > 0) {
            connectedSlaves += count[0];
        } else {
            connectedSlaves += 1;
        }
    }

    public void setMasterReplOffset(int masterReplOffset) {
        this.masterReplOffset = masterReplOffset;
    }

    public void setSecondReplOffset(int secondReplOffset) {
        this.secondReplOffset = secondReplOffset;
    }

    @Override
    public String toString() {
        String CRLF = "\r\n";
        return "role:" + role + CRLF +
                "connected_slaves:" + connectedSlaves + CRLF +
                "master_replid:" + masterReplId + CRLF +
                "master_repl_offset:" + masterReplOffset + CRLF +
                "second_repl_offset:" + secondReplOffset;
    }
}
