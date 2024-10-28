package utils;

import commands.RedisCommand;

import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.Queue;

public final class ClientState {
    private final ByteBuffer readBuffer;
    private final ByteBuffer writeBuffer;
    private final Deque<String> responseQueue;
    private final Queue<RedisCommand> transactionQueue;
    private boolean transactionState = false;

    public ClientState(ByteBuffer readBuffer, ByteBuffer writeBuffer, Deque<String> responseQueue, Queue<RedisCommand> transactionQueue) {
        this.readBuffer = readBuffer;
        this.writeBuffer = writeBuffer;
        this.responseQueue = responseQueue;
        this.transactionQueue = transactionQueue;
    }

    public ByteBuffer readBuffer() {
        return readBuffer;
    }

    public ByteBuffer writeBuffer() {
        return writeBuffer;
    }

    public Deque<String> responseQueue() {
        return responseQueue;
    }

    public Queue<RedisCommand> transactionQueue() {
        return transactionQueue;
    }

    public boolean isInTransaction() {
        return transactionState;
    }

    public void setInTransaction() {
        this.transactionState = true;
    }

    public void endTransaction() {
        this.transactionState = false;
        this.transactionQueue.clear();
    }

}
