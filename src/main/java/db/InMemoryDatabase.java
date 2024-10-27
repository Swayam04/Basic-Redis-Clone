package db;

import java.util.HashMap;
import java.util.Map;

public class InMemoryDatabase {
    private static InMemoryDatabase instance;
    private final Map<String, String> mainTable;

    private InMemoryDatabase() {
        mainTable = new HashMap<>();
    }

    public static InMemoryDatabase getInstance() {
        if (instance == null) {
            instance = new InMemoryDatabase();
        }
        return instance;
    }

    public void addStringData(String key, String value) {
        mainTable.put(key, value);
    }

    public String getStringData(String key) {
        return mainTable.getOrDefault(key, null);
    }

}
