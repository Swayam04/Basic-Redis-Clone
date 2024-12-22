package commands;

import resp.RespEncoder;

import java.util.List;

public class ReplConfCommand extends RedisCommand {

    public ReplConfCommand(String name, List<String> args) {
        super(name, args);
    }

    @Override
    public String execute() {
        return RespEncoder.encode("OK", true);
    }

    @Override
    public void checkSyntax() {
        if (getArgs().size() == 1) {
            throw new IllegalArgumentException("wrong number of arguments for " + name + " command");
        }
    }

    @Override
    public boolean isWriteCommand() {
        return false;
    }

    @Override
    public boolean isReplicaCommand() {
        return true;
    }
}
