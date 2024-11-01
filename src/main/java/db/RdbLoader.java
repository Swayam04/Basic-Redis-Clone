package db;

import core.RedisServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class RdbLoader {
    private static final Logger logger = LoggerFactory.getLogger(RdbLoader.class);

    public static void load() {
        logger.info("Starting RDB load");
        if (RedisServer.currentConfig().properties().containsKey("dir") &&
                RedisServer.currentConfig().properties().containsKey("dbfilename")) {
            loadFromDump(
                    RedisServer.currentConfig().properties().get("dir"),
                    RedisServer.currentConfig().properties().get("dbfilename")
            );
        }
    }

    private static void loadFromDump(String directoryName, String fileName) {
        Path filePath = Path.of(directoryName).resolve(fileName);

        if (!Files.exists(filePath)) {
            logger.info("RDB file does not exist: {}", filePath);
            return;
        }

        try (FileChannel fileChannel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            ByteBuffer buffer = ByteBuffer.allocate((int) fileChannel.size());
            fileChannel.read(buffer);
            buffer.flip();

            // Validate header
            if (!new String(buffer.array(), 0, 5, StandardCharsets.UTF_8).startsWith("REDIS")) {
                logger.warn("Invalid RDB header");
                return;
            }

            // Skip header, auxiliary fields and DB details
            while (buffer.hasRemaining() && (buffer.get() & 0xFF) != 0xFB);
            skipInteger(buffer);
            skipInteger(buffer);

            while (buffer.hasRemaining() && (buffer.get(buffer.position()) & 0xFF) != 0xFF) {
                int marker = buffer.get() & 0xFF;
                long expiry = 0;
                if (marker == 0xFC || marker == 0xFD) {
                    expiry = readExpiry(buffer, marker == 0xFC);
                    marker = buffer.get() & 0xFF;
                }

                if (marker != 0) {
                    logger.warn("Unsupported value type: 0x{}", String.format("%02X", marker));
                    break;
                }

                String key = readString(buffer);
                String value = readString(buffer);

                if (expiry > 0) {
                    LocalDateTime expiryDateTime = expiry <= 2L * Integer.MAX_VALUE ?
                            LocalDateTime.ofInstant(Instant.ofEpochSecond(expiry), ZoneId.systemDefault()) :
                            LocalDateTime.ofInstant(Instant.ofEpochMilli(expiry), ZoneId.systemDefault());
                    InMemoryDatabase.getInstance().addTemporaryStringData(key, value, expiryDateTime);
                } else {
                    InMemoryDatabase.getInstance().addStringData(key, value);
                }
            }
            logger.info("Completed loading RDB file");
        } catch (Exception e) {
            logger.warn("Failed to load RDB file", e);
        }
    }

    private static long readExpiry(ByteBuffer buffer, boolean isMillis) {
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        long expiry = isMillis ? buffer.getLong() : buffer.getInt();
        buffer.order(ByteOrder.BIG_ENDIAN);
        return expiry;
    }

    private static String readString(ByteBuffer buffer) {
        int lengthEncoding = (buffer.get(buffer.position()) & 0xC0) >> 6;

        if (lengthEncoding == 0b11) {
            return String.valueOf(readInteger(buffer));
        }

        int length;
        if (lengthEncoding == 0b00) {
            length = (buffer.get() & 0x3F);
        } else if (lengthEncoding == 0b01) {
            length = ((buffer.getShort() & 0x3FFF));
        } else {
            buffer.get();
            length = buffer.getInt();
        }

        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static long readInteger(ByteBuffer buffer) {
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        long result = switch (buffer.get() & 0xFF) {
            case 0xC0 -> buffer.get();
            case 0xC1 -> buffer.getShort();
            case 0xC2 -> buffer.getInt();
            default -> 0;
        };
        buffer.order(ByteOrder.BIG_ENDIAN);
        return result;
    }

    private static void skipInteger(ByteBuffer buffer) {
        int identifier = buffer.get() & 0xFF;
        if (identifier == 0xC0) buffer.get();
        else if (identifier == 0xC1) buffer.getShort();
        else if (identifier == 0xC2) buffer.getInt();
    }
}
