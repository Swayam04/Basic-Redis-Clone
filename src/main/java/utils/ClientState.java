package utils;

import java.nio.ByteBuffer;
import java.util.Deque;

public record ClientState(ByteBuffer readBuffer, ByteBuffer writeBuffer, Deque<String> responseQueue) {}
