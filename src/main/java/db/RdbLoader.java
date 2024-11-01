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
        logger.info("Starting RDB load");
        if(RedisServer.currentConfig().properties().containsKey("dir") &&
                RedisServer.currentConfig().properties().containsKey("dbfilename")) {
            String dir = RedisServer.currentConfig().properties().get("dir");
            String filename = RedisServer.currentConfig().properties().get("dbfilename");
            logger.info("Loading RDB from directory: {}, filename: {}", dir, filename);
            loadFromDump(dir, filename);
        } else {
            logger.info("No RDB configuration found");
        }
    }

    private static void loadFromDump(String directoryName, String fileName) {
        Path filePath = Path.of(directoryName).resolve(fileName);
        logger.info("Attempting to load RDB file: {}", filePath.toAbsolutePath());

        if(!Files.exists(filePath)) {
            logger.info("RDB file does not exist: {}", filePath);
            return;
        }

        try(FileChannel fileChannel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            logger.info("Opened RDB file, size: {} bytes", fileChannel.size());
            ByteBuffer readBuffer = ByteBuffer.allocate((int) fileChannel.size());
            int bytesRead = fileChannel.read(readBuffer);
            logger.debug("Read {} bytes from file", bytesRead);
            readBuffer.flip();

            String header = readHeader(readBuffer);
            logger.info("Read RDB header: {}", header);
            if(!header.startsWith("REDIS")) {
                logger.warn("Invalid RDB header: {}", header);
                return;
            }

            logger.debug("Skipping auxiliary fields and DB details");
            skipAuxiliaryFieldsAndDBDetails(readBuffer);
            readKeyCounts(readBuffer);
            logger.info("Found {} total keys, {} keys with expiry", numberOfKeys, numberOfKeysWithExpiry);

            for(int i = 0; i < numberOfKeys; i++) {
                logger.debug("Processing key {} of {}", i + 1, numberOfKeys);
                logger.debug("Buffer position before reading key: {}", readBuffer.position());

                int marker = (readBuffer.get() & 0xFF);
                logger.debug("Read marker byte: 0x{}", String.format("%02X", marker));

                long expiry = 0;
                if(marker == 0xFC || marker == 0xFD) {
                    expiry = readExpiry(readBuffer, marker == 0xFC);
                    logger.info("Key has expiry timestamp: {}", expiry);
                }

                int valueType;
                if(expiry > 0) {
                    valueType = readBuffer.get() & 0xFF;
                    logger.debug("Read value type after expiry: 0x{}", String.format("%02X", valueType));
                } else {
                    valueType = marker;
                    logger.debug("Using marker as value type: 0x{}", String.format("%02X", valueType));
                }

                if(valueType != 0) {
                    logger.warn("Unsupported value type: 0x{}", String.format("%02X", valueType));
                    break;
                }

                logger.debug("Reading key at position: {}", readBuffer.position());
                String key = readStringValue(readBuffer);
                logger.debug("Reading value at position: {}", readBuffer.position());
                String value = readStringValue(readBuffer);
                logger.info("Read key-value pair: '{}' = '{}' (key length: {}, value length: {})",
                        key,
                        value.length() > 50 ? value.substring(0, 47) + "..." : value,
                        key.length(),
                        value.length());

                if(expiry > 0) {
                    LocalDateTime expiryDateTime;
                    if(expiry <= 2L * Integer.MAX_VALUE) {
                        expiryDateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(expiry), ZoneId.systemDefault());
                    } else {
                        expiryDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(expiry), ZoneId.systemDefault());
                    }
                    logger.debug("Setting key with expiry: {}", expiryDateTime);
                    InMemoryDatabase.getInstance().addTemporaryStringData(key, value, expiryDateTime);
                } else {
                    logger.debug("Setting key without expiry");
                    InMemoryDatabase.getInstance().addStringData(key, value);
                }
            }
            readBuffer.clear();
            logger.info("Completed loading RDB file");

        } catch (Exception e) {
            logger.warn("Failed to load file {}", fileName, e);
        }
    }

    private static long readExpiry(ByteBuffer buffer, boolean isMillis) {
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        long expiry;
        if (isMillis) {
            expiry = buffer.getLong();
            logger.debug("Read millisecond expiry: {}", expiry);
        } else {
            expiry = buffer.getInt();
            logger.debug("Read second expiry: {}", expiry);
        }
        buffer.order(ByteOrder.BIG_ENDIAN);
        return expiry;
    }

    private static String readStringValue(ByteBuffer buffer) {
        int initialPosition = buffer.position();
        logger.debug("Starting to read string at position: {}", initialPosition);

        int lengthEncoding = (buffer.get(buffer.position()) & 0xC0) >> 6;
        logger.debug("Length encoding bits: {}", String.format("%2b", lengthEncoding));

        int length;
        if(lengthEncoding == 0b00) {
            length = (buffer.get() << 2) >> 2;
            logger.debug("6-bit length encoding: {}", length);
        } else if(lengthEncoding == 0b01) {
            length = (buffer.getShort() << 2) >> 2;
            logger.debug("14-bit length encoding: {}", length);
        } else if(lengthEncoding == 0b10) {
            buffer.get();
            length = buffer.getInt();
            logger.debug("32-bit length encoding: {}", length);
        } else {
            logger.debug("Special encoding detected, reading integer");
            return String.valueOf(readInteger(buffer));
        }

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        logger.debug("Reading {} bytes for string content", length);
        while(length > 0) {
            byte b = buffer.get();
            byteArrayOutputStream.write(b);
            length--;
        }
        String result = byteArrayOutputStream.toString(StandardCharsets.UTF_8);
        logger.debug("Read string from {} to {}: '{}'",
                initialPosition,
                buffer.position(),
                result.length() > 50 ? result.substring(0, 47) + "..." : result);
        return result;
    }

    private static String readHeader(ByteBuffer buffer) {
        logger.debug("Starting to read header at position: {}", buffer.position());
        int nextSectionMarker = 0xFA;
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        while(buffer.hasRemaining()) {
            byte b = buffer.get();
            if((b & 0xFF) == nextSectionMarker) {
                logger.debug("Found header end marker at position: {}", buffer.position() - 1);
                break;
            }
            byteArrayOutputStream.write(b);
        }
        String header = byteArrayOutputStream.toString(StandardCharsets.UTF_8);
        logger.debug("Read header: '{}'", header);
        return header;
    }

    private static void skipAuxiliaryFieldsAndDBDetails(ByteBuffer buffer) {
        logger.debug("Starting to skip auxiliary fields at position: {}", buffer.position());
        int nextSectionMarker = 0xFB;
        int bytesSkipped = 0;
        while(buffer.hasRemaining()) {
            byte b = buffer.get();
            bytesSkipped++;
            if((b & 0xFF) == nextSectionMarker) {
                logger.debug("Found auxiliary section end marker after {} bytes at position: {}",
                        bytesSkipped, buffer.position() - 1);
                break;
            }
        }
    }

    private static void readKeyCounts(ByteBuffer buffer) {
        logger.debug("Reading key counts at position: {}", buffer.position());
        numberOfKeys = readInteger(buffer);
        numberOfKeysWithExpiry = readInteger(buffer);
        logger.debug("Read counts - total keys: {}, keys with expiry: {}",
                numberOfKeys, numberOfKeysWithExpiry);
    }

    private static long readInteger(ByteBuffer buffer) {
        byte identifier = buffer.get();
        int initialPosition = buffer.position();
        logger.debug("Reading integer at position: {}, identifier: 0x{}",
                initialPosition, String.format("%02X", identifier));

        buffer.order(ByteOrder.LITTLE_ENDIAN);
        long result = switch ((identifier & 0xFF)) {
            case 0xC0 -> {
                byte val = buffer.get();
                logger.debug("Read 8-bit integer: {}", val);
                yield val;
            }
            case 0xC1 -> {
                short val = buffer.getShort();
                logger.debug("Read 16-bit integer: {}", val);
                yield val;
            }
            case 0xC2 -> {
                int val = buffer.getInt();
                logger.debug("Read 32-bit integer: {}", val);
                yield val;
            }
            default -> {
                logger.debug("Unknown integer identifier: 0x{}", String.format("%02X", identifier));
                yield 0;
            }
        };
        buffer.order(ByteOrder.BIG_ENDIAN);

        logger.debug("Completed reading integer from {} to {}, value: {}",
                initialPosition - 1, buffer.position(), result);
        return result;
    }
}
