package commands;

import db.InMemoryDatabase;
import resp.RespEncoder;

import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.List;
import java.util.Map;

public class SetCommand extends RedisCommand {
    private static final Map<String, TemporalUnit> TTL_UNITS = Map.of(
            "EX", ChronoUnit.SECONDS,
            "PX", ChronoUnit.MILLIS
    );

    public SetCommand(String name, List<String> args) {
        super(name, args);
    }

    @Override
    public String execute() {
        try {
            validateArguments();
            String key = args.get(0);
            String value = args.get(1);

            if (args.size() == 2) {
                InMemoryDatabase.getInstance().addStringData(key, value);
                return RespEncoder.encode("OK", true);
            }

            String ttlUnit = args.get(2).toUpperCase();
            long duration = parseDuration(args.get(3));
            InMemoryDatabase.getInstance().addTemporaryStringData(
                    key, value, duration, TTL_UNITS.get(ttlUnit)
            );
            return RespEncoder.encode("OK", true);

        } catch (IllegalArgumentException e) {
            return RespEncoder.encode(e);
        }
    }

    private void validateArguments() {
        if (args.size() < 2) {
            throw new IllegalArgumentException("wrong number of arguments for " + name + " command");
        }

        if (args.size() > 2) {
            if (args.size() != 4) {
                throw new IllegalArgumentException("syntax error");
            }
            if (!TTL_UNITS.containsKey(args.get(2).toUpperCase())) {
                throw new IllegalArgumentException("invalid expire unit in '" + name + "' command");
            }
        }
    }

    private long parseDuration(String durationStr) {
        try {
            long duration = Long.parseLong(durationStr);
            if (duration <= 0) {
                throw new IllegalArgumentException("invalid expire time in '" + name + "' command: must be positive");
            }
            return duration;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid expire time in '" + name + "' command");
        }
    }
}
