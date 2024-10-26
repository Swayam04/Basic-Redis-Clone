package core;

import commands.CommandExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resp.RespParser;
import utils.ClientState;
import utils.ParsedCommand;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
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

        logger.info("Accepted connection from {}", getClientInfo(key));

        try {
            client.configureBlocking(false);
            ClientState clientState = new ClientState(
                    ByteBuffer.allocate(config.bufferSize()),
                    ByteBuffer.allocate(config.bufferSize()),
                    new ArrayDeque<>()
            );
            client.register(selector, SelectionKey.OP_READ, clientState);
            logger.debug("Client {} registered for reading", getClientInfo(key));
        } catch (IOException e) {
            logger.error("Error while accepting client connection: ", e);
            client.close();
        }
    }

    public void read(SelectionKey key) throws IOException {
        SocketChannel client = (SocketChannel) key.channel();
        ClientState state = (ClientState) key.attachment();
        ByteBuffer readBuffer = state.readBuffer();
        Deque<String> responseQueue = state.responseQueue();

        try {
            int bytesRead = client.read(readBuffer);
            if (bytesRead == -1) {
                logger.info("Client {} disconnected", getClientInfo(key));
                closeConnection(key);
                return;
            }
            if (bytesRead > 0) {
                List<Optional<ParsedCommand>> parsedCommands = RespParser.parseCommand(readBuffer);
                for (Optional<ParsedCommand> command : parsedCommands) {
                    String response = CommandExecutor.executeCommand(command.orElse(null));
                    logger.info("Response: {}", response);
                    responseQueue.addLast(response);
                }
                readBuffer.compact();
                readBuffer.flip();
                if (!responseQueue.isEmpty()) {
                    key.interestOps(SelectionKey.OP_WRITE);
                    logger.debug("Registered for write after reading {} bytes.", bytesRead);
                }
            }
        } catch (IOException e) {
            logger.error("Error reading from client {}: ", getClientInfo(key), e);
            closeConnection(key);
        }
    }

    public void write(SelectionKey key) throws IOException {
        SocketChannel client = (SocketChannel) key.channel();
        ClientState state = (ClientState) key.attachment();
        Deque<String> responseQueue = state.responseQueue();
        ByteBuffer writeBuffer = state.writeBuffer();

        try {
            while (!responseQueue.isEmpty()) {
                String response = responseQueue.peekFirst();
                byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);
                int remainingBytes = responseBytes.length;
                int offset = 0;

                while (remainingBytes > 0) {
                    writeBuffer.clear();
                    int bytesToWrite = Math.min(writeBuffer.capacity(), remainingBytes);

                    writeBuffer.put(responseBytes, offset, bytesToWrite);
                    writeBuffer.flip();
                    int bytesWritten = client.write(writeBuffer);

                    if (bytesWritten == 0) {
                        if (offset > 0) {
                            String remaining = new String(
                                    responseBytes,
                                    offset,
                                    remainingBytes,
                                    StandardCharsets.UTF_8
                            );
                            responseQueue.removeFirst();
                            responseQueue.addFirst(remaining);
                        }
                        return;
                    }
                    offset += bytesToWrite;
                    remainingBytes -= bytesToWrite;
                }

                responseQueue.removeFirst();
            }
            key.interestOps(SelectionKey.OP_READ);
            logger.debug("Switched back to read mode");
        } catch (IOException e) {
            logger.error("Error writing to client {}: ", getClientInfo(key), e);
            closeConnection(key);
        }
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
