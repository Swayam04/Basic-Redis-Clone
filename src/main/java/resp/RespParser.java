package resp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.ParsedCommand;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;


public class RespParser {
    public static final byte DOLLAR_BYTE = '$';
    public static final byte ASTERISK_BYTE = '*';
    public static final byte PLUS_BYTE = '+';
    public static final byte MINUS_BYTE = '-';
    public static final byte COLON_BYTE = ':';
    public static final byte COMMA_BYTE = ',';
    public static final byte HASH_BYTE = '#';

    private static final Logger logger = LoggerFactory.getLogger(RespParser.class);

    public static List<Optional<ParsedCommand>> parseCommand(ByteBuffer readBuffer) {
        readBuffer.mark();
        byte[] arr = new byte[readBuffer.remaining()];
        readBuffer.get(arr);
        logger.info("All data: {}", new String(arr, StandardCharsets.UTF_8));
        readBuffer.reset();
        List<Optional<ParsedCommand>> parsedCommands = new ArrayList<>();
        while (readBuffer.hasRemaining()) {
            readBuffer.mark();
            try {
                byte b = readBuffer.get();
                logger.info("First byte: {}", b);
                if (b != ASTERISK_BYTE) {
                    parsedCommands.add(Optional.empty());
                    skipUntilCRLF(readBuffer);
                    continue;
                }
                try {
                    @SuppressWarnings("unchecked")
                    List<String> input = (List<String>) parseInput(b, readBuffer);
                    if (input == null || input.isEmpty()) {
                        parsedCommands.add(Optional.empty());
                        continue;
                    }
                    logger.info("Parsed command: {}", input);
                    parsedCommands.add(Optional.of(new ParsedCommand(input.getFirst(), input.subList(1, input.size()))));
                } catch (IllegalArgumentException e) {
                    logger.error(e.getMessage());
                    parsedCommands.add(Optional.empty());
                }
            } catch (BufferUnderflowException | IllegalStateException e) {
                logger.error(e.getMessage());
                readBuffer.reset();
                break;
            }
        }
        return parsedCommands;
    }

    public static Object parseInput(Byte firstByte, ByteBuffer readBuffer) {
        if (!readBuffer.hasRemaining()) {
            throw new IllegalStateException("Incomplete RESP data");
        }
        return switch (firstByte) {
            case PLUS_BYTE -> parseSimpleString(readBuffer);
            case MINUS_BYTE -> parseErrorMessage(readBuffer);
            case COLON_BYTE -> parseInteger(readBuffer);
            case ASTERISK_BYTE -> parseList(readBuffer);
            case DOLLAR_BYTE -> parseBulkString(readBuffer);
            case HASH_BYTE -> parseBool(readBuffer);
            case COMMA_BYTE -> parseDouble(readBuffer);
            default -> throw new IllegalArgumentException("Unknown RESP type: " + (char)firstByte.byteValue());
        };
    }

    private static String readLine(ByteBuffer readBuffer) {
        StringBuilder sb = new StringBuilder();
        boolean foundCR = false;
        while (readBuffer.hasRemaining()) {
            byte b = readBuffer.get();
            if (b == '\r') {
                foundCR = true;
            } else if (b == '\n' && foundCR) {
                return sb.toString();
            } else {
                if (foundCR) {
                    sb.append('\r');
                    foundCR = false;
                }
                sb.append((char) b);
            }
        }
        throw new IllegalStateException("Incomplete line");
    }

    public static String parseErrorMessage(ByteBuffer readBuffer) {
        return readLine(readBuffer);
    }

    private static Long parseInteger(ByteBuffer readBuffer) {
        return Long.parseLong(readLine(readBuffer));
    }

    private static Double parseDouble(ByteBuffer readBuffer) {
        return Double.parseDouble(readLine(readBuffer));
    }

    private static String parseBulkString(ByteBuffer readBuffer) {
        int length = Integer.parseInt(readLine(readBuffer));
        if (length == -1) {
            return null;
        }
        if (length < 0) {
            throw new IllegalArgumentException("Invalid bulk string length: " + length);
        }
        if (readBuffer.remaining() < length + 2) {
            throw new IllegalStateException("Incomplete bulk string");
        }

        byte[] bytes = new byte[length];
        readBuffer.get(bytes);
        String result = new String(bytes);

        if (readBuffer.get() != '\r' || readBuffer.get() != '\n') {
            throw new IllegalStateException("Missing CRLF");
        }
        return result;
    }

    private static String parseSimpleString(ByteBuffer readBuffer) {
        return readLine(readBuffer);
    }

    private static Boolean parseBool(ByteBuffer readBuffer) {
        String value = readLine(readBuffer);
        if ("t".equals(value)) return true;
        if ("f".equals(value)) return false;
        throw new IllegalArgumentException("Invalid boolean value: " + value);
    }

    private static List<?> parseList(ByteBuffer readBuffer) {
        int size = Integer.parseInt(readLine(readBuffer));
        if (size == -1) {
            return null;
        }
        if (size < 0) {
            throw new IllegalArgumentException("Invalid array length: " + size);
        }

        List<Object> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            if (!readBuffer.hasRemaining()) {
                throw new IllegalStateException("Incomplete array");
            }
            list.add(parseInput(readBuffer.get(), readBuffer));
        }
        return list;
    }

    private static void skipUntilCRLF(ByteBuffer readBuffer) {
        boolean foundCR = false;
        while (readBuffer.hasRemaining()) {
            byte b = readBuffer.get();
            if (b == '\r') {
                foundCR = true;
            } else if (b == '\n' && foundCR) {
                return;
            } else {
                foundCR = false;
            }
        }
        throw new IllegalStateException("Incomplete line while skipping");
    }
}
