package db;

import core.RedisServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
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
        return isMillis ? buffer.getLong() : buffer.getInt();
    }

    private static String readStringValue(ByteBuffer buffer) {
        int lengthEncoding = (buffer.get(buffer.position()) & 0xC0) >> 6;
        int length;
        if(lengthEncoding == 0b00) {
             length = buffer.get() << 2;
        } else if(lengthEncoding == 0b01) {
            length = buffer.getShort() << 2;
        } else if(lengthEncoding == 0b10) {
            buffer.get();
            length = buffer.getInt();
        } else {
            return String.valueOf(readInteger(buffer));
        }
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        while(length > 0) {
            byte b = buffer.get();
            byteArrayOutputStream.write(b);
            length--;
        }
        return byteArrayOutputStream.toString(StandardCharsets.UTF_8);
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
        return switch ((identifier & 0xFF)) {
            case 0xC0 -> buffer.get();
            case 0xC1 -> buffer.getShort();
            case 0xC2 -> buffer.getInt();
            default -> 0;
        };
    }

}
