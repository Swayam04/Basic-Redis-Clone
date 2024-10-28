package commands;

import resp.RespEncoder;

import java.util.List;

public class DiscardCommand extends RedisCommand {
    private final boolean inTransaction;

    public DiscardCommand(String name, List<String> args, boolean inTransaction) {
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
    public String execute() {
        if(!inTransaction) {
            return RespEncoder.encode(new IllegalStateException("DISCARD without MULTI"));
        }
        return RespEncoder.encode("OK", true);
    }
}
