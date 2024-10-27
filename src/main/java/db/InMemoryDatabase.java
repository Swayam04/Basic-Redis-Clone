package db;

import java.time.LocalDateTime;
import java.time.temporal.TemporalUnit;
import java.util.HashMap;
import java.util.Map;

public class InMemoryDatabase {
    private static InMemoryDatabase instance;
    private final Map<String, Entry> mainTable;

    private InMemoryDatabase() {
        mainTable = new HashMap<>();
    }

    public static InMemoryDatabase getInstance() {
        if (instance == null) {
            instance = new InMemoryDatabase();
        }
        return instance;
    }

    public void addTemporaryStringData(String key, String value, long expirationDuration, TemporalUnit unit) {
        mainTable.put(key, new Entry(RedisDataType.STRING, value, LocalDateTime.now().plus(expirationDuration, unit)));
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
