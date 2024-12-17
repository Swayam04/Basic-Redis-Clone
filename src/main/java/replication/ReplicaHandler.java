package replication;

import core.RedisServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resp.RespEncoder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

public class ReplicaHandler {
    private static final Logger logger = LoggerFactory.getLogger(ReplicaHandler.class);
    private final String masterHost;
    private final int masterPort;
    private final ByteBuffer readBuffer;
    private SocketChannel masterSocketChannel;
    private Boolean isRunning;

    public ReplicaHandler() {
        String[] masterDetails = RedisServer.currentConfig().properties().get("replicaof").split(" ");
        masterHost = masterDetails[0];
        masterPort = Integer.parseInt(masterDetails[1]);
        readBuffer = ByteBuffer.allocate(RedisServer.currentConfig().bufferSize());
        this.isRunning = false;
    }

    public void start() {
        isRunning = true;
        try {
            connectToMaster();
            performSync();
        } catch (Exception e) {
            logger.error("Error starting replica handler", e);
        } finally {
            stop();
        }
    }

    private void performSync() {
        logger.info("Performing initial sync with master");
        try {
            masterSocketChannel.write(ByteBuffer.wrap(RespEncoder.encode(List.of("PING")).getBytes()));
        } catch (IOException e) {
            logger.error("Error performing sync", e);
        }
        logger.info("Initial sync completed");
    }

    public void connectToMaster() throws IOException {
        logger.info("Connecting to master at {}", masterHost + ":" + masterPort);
        masterSocketChannel = SocketChannel.open(new InetSocketAddress(masterHost, masterPort));
        masterSocketChannel.configureBlocking(true);
        logger.info("Connected to master at {}", masterHost + ":" + masterPort);
    }

    public void stop() {
        isRunning = false;
        try {
            if (masterSocketChannel != null && masterSocketChannel.isOpen()) {
                masterSocketChannel.close();
            }
        } catch (IOException e) {
            logger.error("Error while closing master connection", e);
        }
    }
}
