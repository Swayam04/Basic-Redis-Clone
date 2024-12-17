package core;

import db.InMemoryDatabase;
import db.RdbLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import replication.ReplicaHandler;
import replication.ReplicationInfo;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class RedisServer {
    private static final Logger logger = LoggerFactory.getLogger(RedisServer.class);

    private static ServerConfig globalConfig;
    private final ServerSocketChannel serverChannel;
    private final Selector selector;
    private final EventLoop eventLoop;
    private final ReplicaHandler replicaHandler;
    private final AtomicBoolean isRunning;

    public record ServerConfig(int port, int bufferSize, long timeout, Map<String, String> properties) {
    }

    private static final class ReplicationInfoHolder {
        static final ReplicationInfo replicationInfo = new ReplicationInfo();
    }

    public RedisServer(ServerConfig config) throws IOException {
        globalConfig = config;
        this.isRunning = new AtomicBoolean(false);
        this.selector = Selector.open();
        this.serverChannel = ServerSocketChannel.open();
        if(globalConfig.properties().containsKey("replicaof")) {
            this.replicaHandler = new ReplicaHandler();
        } else {
            this.replicaHandler = null;
        }
        this.eventLoop = new EventLoop.Builder().
                selector(this.selector).
                isRunning(isRunning).build();
    }

    public static ServerConfig currentConfig() {
        return globalConfig;
    }

    public static ReplicationInfo getReplicationInfo() {
        return ReplicationInfoHolder.replicationInfo;
    }

    public void start() {
        if(!isRunning.compareAndSet(false, true)) {
            logger.info("Redis server is already running.");
            return;
        }
        try {
            if(replicaHandler != null) {
                logger.info("Redis server starting in replica mode");
                replicaHandler.start();
            }
            serverChannel.configureBlocking(false);
            serverChannel.socket().setReuseAddress(true);
            serverChannel.socket().bind(new InetSocketAddress(globalConfig.port));

            serverChannel.register(selector, SelectionKey.OP_ACCEPT);
            logger.info("Redis server starting on port {}", globalConfig.port);
            logger.info("Configuration: bufferSize = {}, commandTimeout = {}ms", globalConfig.bufferSize, globalConfig.timeout);

            RdbLoader.load();
            eventLoop.start();
        } catch(IOException e) {
            logger.error("Failed to start server", e);
            shutdown();
            throw new RuntimeException("Redis server startup failed" ,e);
        }
    }

    public void shutdown() {
        if(!isRunning.compareAndSet(true, false)) {
            logger.info("Redis server is already shut down.");
            return;
        }
        try {
            if(replicaHandler != null) {
                replicaHandler.stop();
            }
            eventLoop.stop();
            selector.close();
            serverChannel.close();
            InMemoryDatabase.getInstance().clear();
            logger.info("Redis server shut down.");
        } catch (IOException e) {
            logger.error("Failed to shutdown server", e);
        }
    }

}
