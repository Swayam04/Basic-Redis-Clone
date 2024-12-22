package resp;

import commands.RedisCommand;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public final class RespEncoder {
    private static final String CRLF = "\r\n";

    public static <T> String encode(T response, boolean... isSimple) {
        if (response == null) {
            return "$-1" + CRLF;
        }
        boolean simpleString = isSimple.length > 0 && isSimple[0];

        return switch (response) {
            case Exception e -> encodeError(e);
            case String s -> simpleString ? encodeSimpleString(s) : encodeBulkString(s);
            case Integer i -> encodeNumber(i);
            case Long l -> encodeNumber(l);
            case Float f -> encodeNumber(f);
            case Double d -> encodeNumber(d);
            case Boolean b -> encodeBoolean(b);
            case List<?> list -> encodeList(list);
            default -> throw new IllegalArgumentException("Unsupported type: " + response.getClass());
        };
    }

    private static String encodeError(Exception e) {
        return "-ERR " + e.getMessage() + CRLF;
    }

    private static String encodeNumber(Number number) {
        if (number instanceof Double || number instanceof Float) {
            return "," + (number.doubleValue() >= 0 ? "+" : "-") + number + CRLF;
        }
        return ":" + (number.longValue() >= 0 ? "+" : "-") + number + CRLF;
    }

    private static String encodeBoolean(Boolean bool) {
        return ":" + (bool ? "t" : "f") + CRLF;
    }

    private static String encodeSimpleString(String string) {
        if (string.contains(CRLF)) {
            throw new IllegalArgumentException("Simple string cannot contain CRLF");
        }
        return "+" + string + CRLF;
    }

    private static String encodeBulkString(String string) {
        return "$" + string.length() + CRLF + string + CRLF;
    }

    private static String encodeList(List<?> items) {
        if (items == null) {
            return "$-1" + CRLF;
        }
        StringBuilder encodedList = new StringBuilder()
                .append("*")
                .append(items.size())
                .append(CRLF);
        for (Object item : items) {
            if (item == null) {
                encodedList.append("$-1").append(CRLF);
                continue;
            }
            encodedList.append(encode(item));
        }
        return encodedList.toString();
    }

    public static String encodeTransaction(List<String> transaction) {
        StringBuilder encodedTransaction = new StringBuilder();
        encodedTransaction.append("*").append(transaction.size()).append(CRLF);
        for (String item : transaction) {
            encodedTransaction.append(item);
        }
        return encodedTransaction.toString();
    }

    public static String encodeSafeRdb(String base64RdbContent) {
        try {
            byte[] rdbBytes = Base64.getDecoder().decode(base64RdbContent);
            String binaryData = new String(rdbBytes, StandardCharsets.ISO_8859_1);

            return "$" + rdbBytes.length + CRLF +
                    binaryData;
        } catch (Exception e) {
            throw new RuntimeException("Failed to encode RDB data", e);
        }
    }

    public static String encodeCommand(RedisCommand redisCommand) {
        List<String> command = new ArrayList<>();
        command.add(redisCommand.getName());
        command.addAll(redisCommand.getArgs());
        return encodeList(command);
    }
}