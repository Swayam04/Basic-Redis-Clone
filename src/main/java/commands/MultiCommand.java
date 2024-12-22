package commands;

import resp.RespEncoder;

import java.util.List;

public class MultiCommand extends RedisCommand {
    private final boolean inTransaction;

    public MultiCommand(String name, List<String> args, boolean inTransaction) {
        super(name, args);
        this.inTransaction = inTransaction;
    }

    @Override
    public void checkSyntax() {
        if(!args.isEmpty()) {
            throw new IllegalArgumentException("wrong number of arguments for '" + name + "' command");
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
        if(inTransaction) {
            return RespEncoder.encode(new IllegalStateException("MULTI calls can’t be nested"));
        }
        return RespEncoder.encode("OK", true);
    }

}
