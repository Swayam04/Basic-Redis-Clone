package commands;

import resp.RespEncoder;

import java.util.List;

public class PingCommand extends RedisCommand {

    public PingCommand(String name, List<String> arguments) {
        super(name, arguments);
    }

    @Override
    public void checkSyntax() {
        if(args.size() > 1) {
            throw  new IllegalArgumentException("wrong number of arguments for " + name + " command");
        }
    }

    @Override
    public boolean isWriteCommand() {
        return false;
    }

    @Override
    public boolean isReplicaCommand() {
        return false;
    }

    @Override
    public String execute() {
        if(args.size() == 1) {
            return RespEncoder.encode(args.getFirst(), true);
        } else {
            return RespEncoder.encode("PONG", true);
        }
    }
}
