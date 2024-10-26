package commands;

import resp.RespEncoder;

import java.util.List;
import java.util.Objects;

public class PingCommand extends RedisCommand {

    public PingCommand(String name, List<String> arguments) {
        super(name, arguments);
    }

    @Override
    public String execute() {
        if(args.size() > 1) {
            return RespEncoder.encode(new IllegalArgumentException("wrong number of arguments for " + name + " command"));
        }
        return RespEncoder.encode(Objects.requireNonNullElse(args.getFirst(), "PONG"), true);
    }
}
