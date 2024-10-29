package core;

import db.InMemoryDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final AtomicBoolean isRunning;

    public record ServerConfig(int port, int bufferSize, long timeout, Map<String, String> properties) {
    }

    public RedisServer(ServerConfig config) throws IOException {
        globalConfig = config;
        this.isRunning = new AtomicBoolean(false);
        this.selector = Selector.open();
        this.serverChannel = ServerSocketChannel.open();
        this.eventLoop = new EventLoop.Builder().
                            selector(this.selector).
                            isRunning(isRunning).build();
    }

    public static ServerConfig currentConfig() {
        return globalConfig;
    }

    public void start() {
        if(!isRunning.compareAndSet(false, true)) {
            logger.info("Redis server is already running.");
            return;
        }
        try {
            serverChannel.configureBlocking(false);
            serverChannel.socket().setReuseAddress(true);
            serverChannel.socket().bind(new InetSocketAddress(globalConfig.port));

            serverChannel.register(selector, SelectionKey.OP_ACCEPT);
            logger.info("Redis server starting on port {}", globalConfig.port);
            logger.info("Configuration: bufferSize = {}, commandTimeout = {}ms", globalConfig.bufferSize, globalConfig.timeout);

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
