package commands;

import db.InMemoryDatabase;
import resp.RespEncoder;

import java.util.List;

public class IncrCommand extends RedisCommand {

    public IncrCommand(String name, List<String> args) {
        super(name, args);
    }

    @Override
    public void checkSyntax() {
        if(args.size() != 1) {
            throw new IllegalArgumentException("wrong number of arguments for command");
        }
    }

    @Override
    public String execute() {
        try {
            String key = args.getFirst();
            String value = InMemoryDatabase.getInstance().getStringData(key);
            Long incrementedLongValue = value != null ? getLongValue(value) + 1 : 1;
            InMemoryDatabase.getInstance().addStringData(key, String.valueOf(incrementedLongValue));
            return RespEncoder.encode(incrementedLongValue);
        } catch (IllegalArgumentException e) {
            return RespEncoder.encode(e);
        }
    }

    private long getLongValue(String value) {
        try {
            long longValue = Long.parseLong(value);
            if(longValue == Long.MAX_VALUE) {
                throw new IllegalArgumentException("increment or decrement would overflow");
            }
            return longValue;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("value is not an integer or out of range");
        }
    }
}
