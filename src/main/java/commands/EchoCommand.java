package commands;

import resp.RespEncoder;

import java.util.List;

public class EchoCommand extends RedisCommand {

    public EchoCommand(String name, List<String> args) {
        super(name, args);
    }

    @Override
    public String execute() {
        if(args.size() != 1) {
            return RespEncoder.encode(new IllegalArgumentException("wrong number of arguments for " + name + " command"));
        }
        return RespEncoder.encode(args.getFirst());
    }

}
