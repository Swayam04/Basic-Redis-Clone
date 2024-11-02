package db;

import java.time.LocalDateTime;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class InMemoryDatabase {
    private final Map<String, Entry> mainTable;

    private InMemoryDatabase() {
        mainTable = new HashMap<>();
    }

    private static final class InstanceHolder {
        static final InMemoryDatabase INSTANCE = new InMemoryDatabase();
    }

    public static InMemoryDatabase getInstance() {
        return InstanceHolder.INSTANCE;
    }

    public void addTemporaryStringData(String key, String value, LocalDateTime dateTime) {
        mainTable.put(key, new Entry(RedisDataType.STRING, value, dateTime));
    }

    public void addStringData(String key, String value) {
        mainTable.put(key, new Entry(RedisDataType.STRING, value, null));
    }

    public String getStringData(String key) {
        Entry entry = mainTable.get(key);
        if (entry == null || entry.dataType() != RedisDataType.STRING) {
            return null;
        }
        if (entry.expirationDateTime() == null ||
                entry.expirationDateTime().isAfter(LocalDateTime.now())) {
            return (String) entry.value();
        }
        mainTable.remove(key);
        return null;
    }

    public List<String> getKeysMatchingPattern(String regexPattern) {
        try {
            List<String> matchingKeys = new ArrayList<>();
            Pattern pattern = Pattern.compile(regexPattern);

            for(Map.Entry<String, Entry> entry : mainTable.entrySet()) {
                String key = entry.getKey();
                if (pattern.matcher(key).matches()) {
                    matchingKeys.add(key);
                }
            }
            return matchingKeys;
        } catch (PatternSyntaxException e) {
            return Collections.emptyList();
        }
    }

    private record Entry(RedisDataType dataType, Object value, LocalDateTime expirationDateTime) {
    }

    private enum RedisDataType {
        STRING,
        LIST,
        HASH,
        SET,
        JSON,
        STREAM
    }

    public void clear() {
        mainTable.clear();
    }

}
