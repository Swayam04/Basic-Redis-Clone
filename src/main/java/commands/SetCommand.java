package commands;

import db.InMemoryDatabase;
import resp.RespEncoder;

import java.util.List;

public class SetCommand extends RedisCommand {

    public SetCommand(String name, List<String> args) {
        super(name, args);
    }

    @Override
    public String execute() {
        if(args.size() < 2) {
            return RespEncoder.encodeError(new IllegalArgumentException("wrong number of arguments for " + name + " command"));
        }
        InMemoryDatabase.getInstance().addStringData(args.get(0), args.get(1));
        return RespEncoder.encode("OK", true);
    }
}
