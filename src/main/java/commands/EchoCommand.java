package commands;

import resp.RespEncoder;

import java.util.List;

public class EchoCommand extends RedisCommand {

    public EchoCommand(String name, List<String> args) {
        super(name, args);
    }

    @Override
    public void checkSyntax() {
        if(args.size() != 1) {
            throw new IllegalArgumentException("wrong number of arguments for " + name + " command");
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
        return RespEncoder.encode(args.getFirst());
    }

}
