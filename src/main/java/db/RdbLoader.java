package db;

import core.RedisServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
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
    private static long numberOfKeys;
    private static long numberOfKeysWithExpiry;

    public static void load() {
        if(RedisServer.currentConfig().properties().containsKey("dir") &&
                RedisServer.currentConfig().properties().containsKey("dbfilename")) {
            loadFromDump(RedisServer.currentConfig().properties().get("dir"),
                    RedisServer.currentConfig().properties().get("dbfilename"));
        }
    }

    private static void loadFromDump(String directoryName, String fileName) {
        Path filePath = Path.of(directoryName).resolve(fileName);
        if(!Files.exists(filePath)) {
            return;
        }
        try(FileChannel fileChannel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            ByteBuffer readBuffer = ByteBuffer.allocate((int) fileChannel.size());
            fileChannel.read(readBuffer);
            readBuffer.flip();

            String header = readHeader(readBuffer);
            if(!header.startsWith("REDIS")) {
                logger.info("File {} not in valid RDB format", fileName);
                return;
            }
            skipAuxiliaryFieldsAndDBDetails(readBuffer);
            readKeyCounts(readBuffer);

            for(int i = 0; i < numberOfKeys; i++) {
                int marker = (readBuffer.get() & 0xFF);
                long expiry = 0;
                if(marker == 0xFC || marker == 0xFD) {
                    expiry = readExpiry(readBuffer, marker == 0xFC);
                    logger.info("rdb key expiry: {}", expiry);

                }
                int valueType;
                if(expiry > 0) {
                    valueType = readBuffer.get() & 0xFF;
                } else {
                    valueType = marker;
                }
                if(valueType != 0) {
                    logger.info("Value type {} not supported", valueType);
                    break;
                }
                String key = readStringValue(readBuffer);
                String value = readStringValue(readBuffer);
                logger.info("Key: {}, Value: {}", key, value);
                if(expiry > 0) {
                    LocalDateTime expiryDateTime;
                    if(expiry <= 2L * Integer.MAX_VALUE) {
                        expiryDateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(expiry), ZoneId.systemDefault());
                    } else {
                        expiryDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(expiry), ZoneId.systemDefault());
                    }
                    InMemoryDatabase.getInstance().addTemporaryStringData(key, value, expiryDateTime);
                } else {
                    InMemoryDatabase.getInstance().addStringData(key, value);
                }
            }
            readBuffer.clear();
        } catch (Exception e) {
            logger.warn("Failed to load file {}", fileName, e);
        }
    }

    private static long readExpiry(ByteBuffer buffer, boolean isMillis) {
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        long expiry = isMillis ? buffer.getLong() : buffer.getInt();
        buffer.order(ByteOrder.BIG_ENDIAN);
        return expiry;
    }

    private static String readStringValue(ByteBuffer buffer) {
        logger.debug("Starting to read string value at buffer position: {}", buffer.position());

        // Peek at the first byte without consuming
        int firstByte = buffer.get(buffer.position()) & 0xFF;
        int lengthEncoding = (firstByte & 0xC0) >> 6;

        logger.debug("First byte: 0x{}, length encoding: {}", String.format("%02X", firstByte), lengthEncoding);

        // Special encoding for integers (11 prefix)
        if (lengthEncoding == 0b11) {
            logger.debug("Detected special integer encoding");
            return String.valueOf(readInteger(buffer));
        }

        // Regular string length encoding
        int length;
        if (lengthEncoding == 0b00) {
            // Use first byte, mask out length encoding bits
            buffer.get(); // consume the byte we peeked at
            length = firstByte & 0x3F;
            logger.debug("6-bit length encoding: {}", length);
        }
        else if (lengthEncoding == 0b01) {
            // Read 14-bit length
            buffer.get(); // consume first byte
            int secondByte = buffer.get() & 0xFF;
            length = ((firstByte & 0x3F) << 8) | secondByte;
            logger.debug("14-bit length encoding: {}", length);
        }
        else {
            // Read 32-bit length
            buffer.get(); // consume first byte
            length = buffer.getInt();
            logger.debug("32-bit length encoding: {}", length);
        }

        // Validate length
        if (length < 0) {
            throw new IllegalStateException("Invalid negative length: " + length);
        }
        if (length > buffer.remaining()) {
            throw new IllegalStateException(
                    String.format("Length %d exceeds remaining buffer size %d", length, buffer.remaining())
            );
        }

        // Read string content
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        String result = new String(bytes, StandardCharsets.UTF_8);

        logger.debug("Read string of length {}: {}", length,
                result.length() > 50 ? result.substring(0, 47) + "..." : result);

        return result;
    }

    private static String readHeader(ByteBuffer buffer) {
        int nextSectionMarker = 0xFA;
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        while(buffer.hasRemaining()) {
            byte b = buffer.get();
            if((b & 0xFF) == nextSectionMarker) {
                break;
            }
            byteArrayOutputStream.write(b);
        }
        return byteArrayOutputStream.toString(StandardCharsets.UTF_8);
    }

    private static void skipAuxiliaryFieldsAndDBDetails(ByteBuffer buffer) {
        int nextSectionMarker = 0xFB;
        while(buffer.hasRemaining()) {
            byte b = buffer.get();
            if((b & 0xFF) == nextSectionMarker) {
                break;
            }
        }
    }

    private static void readKeyCounts(ByteBuffer buffer) {
        numberOfKeys = readInteger(buffer);
        numberOfKeysWithExpiry = readInteger(buffer);
    }

    private static long readInteger(ByteBuffer buffer) {
        byte identifier = buffer.get();
        int encoding = identifier & 0x3F;

        logger.debug("Reading integer with encoding: 0x{}", String.format("%02X", encoding));

        buffer.order(ByteOrder.LITTLE_ENDIAN);
        try {
            // For encoded integers in string representation
            // Encoding values 0-3 are special cases defined in Redis RDB format
            long result = switch (encoding) {
                case 0 -> {
                    byte val = buffer.get();
                    logger.debug("Read 8-bit integer: {}", val);
                    yield val;
                }
                case 1 -> {
                    short val = buffer.getShort();
                    logger.debug("Read 16-bit integer: {}", val);
                    yield val;
                }
                case 2 -> {
                    int val = buffer.getInt();
                    logger.debug("Read 32-bit integer: {}", val);
                    yield val;
                }
                // Handle encoding 3 for string representation of integers
                case 3 -> {
                    long val = buffer.getLong();
                    logger.debug("Read 64-bit integer: {}", val);
                    yield val;
                }
                default -> {
                    // For other encodings, treat as regular integer
                    logger.debug("Using default integer encoding for: 0x{}", String.format("%02X", encoding));
                    yield encoding;
                }
            };
            buffer.order(ByteOrder.BIG_ENDIAN);
            return result;
        } catch (Exception e) {
            logger.warn("Error reading integer with encoding 0x{}: {}",
                    String.format("%02X", encoding), e.getMessage());
            buffer.order(ByteOrder.BIG_ENDIAN);
            // Return the encoding itself as the value for non-standard cases
            return encoding;
        }
    }

}
