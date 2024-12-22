package replication;

import core.RedisServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resp.RespEncoder;
import utils.ClientState;
import utils.ClientType;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.LinkedList;
import java.util.List;

public class ReplicaHandler {
    private static final Logger logger = LoggerFactory.getLogger(ReplicaHandler.class);
    private final String masterHost;
    private final int masterPort;
    private SocketChannel masterSocketChannel;
    private final ByteBuffer buffer;
    private String masterReplId = "?";
    private int masterOffset = -1;

    public ReplicaHandler() {
        String[] masterDetails = RedisServer.currentConfig().properties().get("replicaof").split(" ");
        this.masterHost = masterDetails[0];
        this.masterPort = Integer.parseInt(masterDetails[1]);
        this.buffer = ByteBuffer.allocate(RedisServer.currentConfig().bufferSize());
    }

    public void start(Selector selector) {
        try {
            connectToMaster();
            performHandshake();
            registerWithSelector(selector);
        } catch (Exception e) {
            logger.error("Error starting replica handler", e);
        } finally {
            stop();
        }
    }

    private void registerWithSelector(Selector selector) throws IOException {
        ClientState clientState = new ClientState(
                ByteBuffer.allocateDirect(RedisServer.currentConfig().bufferSize()),
                ByteBuffer.allocateDirect(RedisServer.currentConfig().bufferSize()),
                new ArrayDeque<>(),
                new LinkedList<>()
        );
        clientState.setClientType(ClientType.MASTER);
        masterSocketChannel.register(selector, SelectionKey.OP_READ, clientState);
    }

    private void performHandshake() throws IOException {
        logger.info("Performing initial sync with master");
        sendCommand("PING", "+PONG\r\n");
        sendCommand("REPLCONF listening-port " + RedisServer.currentConfig().port(), "+OK\r\n");
        sendCommand("REPLCONF capa psync2", "+OK\r\n");
        sendPSync();
        logger.info("Initial sync completed");
    }

    private void sendPSync() throws IOException {
        String pSync = String.format("PSYNC %s %d", masterReplId, masterOffset);
        buffer.clear();
        buffer.put(RespEncoder.encode(List.of(pSync.split(" "))).getBytes());
        buffer.flip();
        masterSocketChannel.write(buffer);

        buffer.clear();
        int bytesRead = masterSocketChannel.read(buffer);
        if (bytesRead > 0) {
            String response = new String(buffer.array(), 0, bytesRead);
            if (response.contains("FULLRESYNC")) {
                String[] responses = response.substring(1, response.length() - 4).split(" ");
//                masterReplId = responses[1].trim();
//                masterOffset = Integer.parseInt(responses[2].trim());
            } else {
                throw new IOException("Invalid PSYNC response: " + response);
            }
        }
    }

    private void sendCommand(String command, String expectedResponse) throws IOException {
        buffer.clear();
        buffer.put(RespEncoder.encode(List.of(command.split(" "))).getBytes());
        buffer.flip();
        masterSocketChannel.write(buffer);

        buffer.clear();
        int bytesRead = masterSocketChannel.read(buffer);
        if (bytesRead > 0) {
            String response = new String(buffer.array(), 0, bytesRead);
            if (!response.equals(expectedResponse)) {
                throw new IOException("Invalid response for " + command + ": " + response);
            }
        } else if (bytesRead == -1) {
            throw new IOException("Connection closed while reading response for: " + command);
        }
    }

    public void connectToMaster() throws IOException {
        logger.info("Connecting to master at {}:{}", masterHost, masterPort);
        masterSocketChannel = SocketChannel.open(new InetSocketAddress(masterHost, masterPort));
        masterSocketChannel.configureBlocking(true);
        logger.info("Connected to master");
    }

    public void stop() {
        try {
            if (masterSocketChannel != null && masterSocketChannel.isOpen()) {
                masterSocketChannel.close();
            }
        } catch (IOException e) {
            logger.error("Error while closing master connection", e);
        }
    }
}