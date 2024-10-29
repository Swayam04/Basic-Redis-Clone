package commands;

import resp.RespEncoder;
import core.RedisServer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ConfigCommand extends RedisCommand {
    private static final Map<String, String> VALID_METHODS = Map.of(
            "get", "Get the values of configuration parameters pattern matches the glob-style pattern.",
            "set", "Set configuration parameters to their respective values",
            "help", "Show helpful text about the different subcommands");

    public ConfigCommand(String name, List<String> args) {
        super(name, args);
    }

    @Override
    public String execute() {
        if (args.isEmpty()) {
            throw new IllegalArgumentException("wrong number of arguments for '" + name + "' command");
        }

        String methodName = args.getFirst().toLowerCase();
        if (!VALID_METHODS.containsKey(methodName)) {
            throw new IllegalArgumentException("unknown CONFIG subcommand '" + methodName + "'");
        }

        List<String> subCommandArgs = args.subList(1, args.size());
        
        return switch (methodName) {
            case "get" -> executeGet(subCommandArgs);
            case "set" -> executeSet(subCommandArgs);
            case "help" -> executeHelp(subCommandArgs);
            default -> RespEncoder.encode(null);
        };
    }

    private String executeGet(List<String> args) {
        if (args.isEmpty()) {
            throw new IllegalArgumentException("wrong number of arguments for CONFIG GET");
        }
        String pattern = args.getFirst();
        Map<String, String> config = RedisServer.currentConfig().properties();
        List<String> response = new ArrayList<>();
        if (pattern.equals("*")) {
            for (Map.Entry<String, String> entry : config.entrySet()) {
                response.add(entry.getKey());
                response.add(entry.getValue());
            }
        } else {
            String value = config.get(pattern);
            if (value != null) {
                response.add(pattern);
                response.add(value);
            }
        }
        return RespEncoder.encode(response);
    }

    private String executeSet(List<String> args) {
        if (args.size() != 2) {
            throw new IllegalArgumentException("wrong number of arguments for CONFIG SET");
        }
        String parameter = args.get(0);
        String value = args.get(1);
        if (!RedisServer.currentConfig().properties().containsKey(parameter)) {
            throw new IllegalArgumentException("invalid config parameter '" + parameter + "'");
        }
        RedisServer.currentConfig().properties().put(parameter, value);
        return RespEncoder.encode("OK");
    }

    private String executeHelp(List<String> args) {
        if(!args.isEmpty()) {
            throw new IllegalArgumentException("wrong number of arguments for CONFIG HELP");
        }
        List<String> response = new ArrayList<>();
        for(Map.Entry<String, String> entry : VALID_METHODS.entrySet()) {
            response.add("CONFIG " + entry.getKey().toUpperCase());
            response.add(entry.getValue());
        }
        return RespEncoder.encode(response);
    }

    @Override
    public void checkSyntax() {
        if (args.isEmpty()) {
            throw new IllegalArgumentException("wrong number of arguments for '" + name + "' command");
        }
    }
}