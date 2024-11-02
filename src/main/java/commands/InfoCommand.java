package commands;

import core.RedisServer;
import resp.RespEncoder;

import java.util.List;

public class InfoCommand extends RedisCommand {

    public InfoCommand(String name, List<String> args) {
        super(name, args);
    }

    @Override
    public void checkSyntax() {
        if(args.size() > 1 ||
                (!args.isEmpty() && !args.getFirst().equalsIgnoreCase("replication"))) {
            throw new UnsupportedOperationException("");
        }
    }

    @Override
    public String execute() {
        return RespEncoder.encode(RedisServer.getReplicationInfo().toString());
    }

}
