package core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class RedisServer {
    private static final Logger logger = LoggerFactory.getLogger(RedisServer.class);

    private final ServerConfig config;
    private final ServerSocketChannel serverChannel;
    private final Selector selector;
    private final EventLoop eventLoop;

    private final AtomicBoolean isRunning;

    public record ServerConfig(int port, int bufferSize, long timeout) {
    }

    public RedisServer(ServerConfig config) throws IOException {
        this.config = config;
        this.isRunning = new AtomicBoolean(false);
        this.selector = Selector.open();
        this.serverChannel = ServerSocketChannel.open();
        this.eventLoop = new EventLoop.Builder().
                            selector(this.selector).
                            config(this.config).
                            isRunning(isRunning).build();
    }

    public void start() {
        if(!isRunning.compareAndSet(false, true)) {
            logger.info("Redis server is already running.");
            return;
        }
        try {
            serverChannel.configureBlocking(false);
            serverChannel.socket().setReuseAddress(true);
            serverChannel.socket().bind(new InetSocketAddress(config.port));

            serverChannel.register(selector, SelectionKey.OP_ACCEPT);
            logger.info("Redis server starting on port {}", config.port);
            logger.info("Configuration: bufferSize={}, commandTimeout={}ms", config.bufferSize, config.timeout);

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
            logger.info("Redis server shut down.");
        } catch (IOException e) {
            logger.error("Failed to shutdown server", e);
        }
    }

}
