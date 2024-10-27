package commands;

import db.InMemoryDatabase;
import resp.RespEncoder;

import java.util.List;

public class GetCommand extends RedisCommand {

    public GetCommand(String name, List<String> args) {
        super(name, args);
    }

    @Override
    public String execute() {
        if(args.size() != 1) {
            return RespEncoder.encodeError(new IllegalArgumentException("wrong number of arguments for " + name + " command"));
        }
        return RespEncoder.encode(InMemoryDatabase.getInstance().getStringData(args.getFirst()));
    }

}
