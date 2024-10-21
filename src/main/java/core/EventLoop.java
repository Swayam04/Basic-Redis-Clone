package core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

public class EventLoop {

    public static class Builder {
        private Selector selector;
        private RedisServer.ServerConfig config;
        private AtomicBoolean isRunning;

        public Builder selector(Selector selector) {
            this.selector = selector;
            return this;
        }

        public Builder isRunning(AtomicBoolean isRunning) {
            this.isRunning = isRunning;
            return this;
        }

        public Builder config(RedisServer.ServerConfig config) {
            this.config = config;
            return this;
        }

        public EventLoop build() {
            return new EventLoop(this);
        }
    }

    private final Selector selector;
    private final RedisServer.ServerConfig config;
    private final AtomicBoolean isRunning;
    private static final Logger logger = LoggerFactory.getLogger(EventLoop.class);

    private EventLoop(Builder builder) {
        selector = builder.selector;
        config = builder.config;
        isRunning = builder.isRunning;
    }

    public void start() {
        logger.info("Starting event loop");
        while(isRunning.get()) {
            try {
                int readyOps = selector.select(config.timeout());
                if(readyOps > 0) {
                    processSelectedKeys();
                }
            } catch (IOException e) {
                logger.error("Error while starting event loop: ", e);
            }
        }
    }

    public void stop() {
        logger.info("Stopping event loop");
        selector.wakeup();
    }

    public void processSelectedKeys() {
        Iterator<SelectionKey> keys = selector.selectedKeys().iterator();

        while(keys.hasNext()) {
            SelectionKey key = keys.next();
            keys.remove();

            try {
                if(!key.isValid()) {
                    continue;
                }

                if(key.isAcceptable()) {
                    logger.info("Accepting new connection");
                    acceptConnection(key);
                } else if(key.isReadable()) {
                    logger.info("Reading from client: {}", getClientInfo(key));
                    read(key);
                } else if(key.isWritable()) {
                    logger.info("Writing to client: {}", getClientInfo(key));
                    write(key);
                }
            } catch (IOException e) {
                logger.error("Failed to configure new client connection: ", e);
                closeConnection(key);
            }
        }
    }

    public void acceptConnection(SelectionKey key) throws IOException {
        ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
        SocketChannel client = serverChannel.accept();
        String clientAddress = client.getRemoteAddress().toString();

        logger.info("Accepted connection from {}", clientAddress);

        try {
            client.configureBlocking(false);
            ByteBuffer readBuffer = ByteBuffer.allocate(config.bufferSize());
            client.register(selector, SelectionKey.OP_READ, readBuffer);

            logger.debug("Client {} registered for reading", clientAddress);
        } catch (IOException e) {
            logger.error("Error while accepting client connection: ", e);
            client.close();
        }
    }

    public void read(SelectionKey key) throws IOException {
        SocketChannel client = (SocketChannel) key.channel();
        ByteBuffer readBuffer = (ByteBuffer) key.attachment();

        int bytesRead = client.read(readBuffer);
        if(bytesRead == -1) {
            logger.info("Client {} disconnected", client.getRemoteAddress());
            closeConnection(key);
            return;
        }
        readBuffer.flip();
        key.interestOps(SelectionKey.OP_WRITE);
        logger.debug("Registered for write after reading {}", bytesRead);
    }

    public void write(SelectionKey key) throws IOException {
        SocketChannel client = (SocketChannel) key.channel();
        ByteBuffer writeBuffer = ByteBuffer.wrap("+PONG\r\n".getBytes());
        client.write(writeBuffer);
        key.interestOps(SelectionKey.OP_READ);
        ByteBuffer readBuffer = (ByteBuffer) key.attachment();
        readBuffer.clear();
        logger.debug("Responded with PONG. Switched back to read mode");
    }

    private  void closeConnection(SelectionKey key) {
        try {
            key.cancel();
            key.channel().close();
            logger.info("Closing connection");
        } catch (IOException e) {
            logger.error("Error while closing connection: ", e);
        }
    }

    private String getClientInfo(SelectionKey key) {
        try {
            if (key.channel() instanceof SocketChannel channel) {
                return channel.getRemoteAddress().toString();
            }
        } catch (IOException ex) {
            logger.debug("Could not get client info", ex);
        }
        return "unknown";
    }

}
