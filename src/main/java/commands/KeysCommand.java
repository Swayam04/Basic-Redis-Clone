package commands;

import db.InMemoryDatabase;
import resp.RespEncoder;

import java.util.List;

public class KeysCommand extends RedisCommand {

    public KeysCommand(String name, List<String> args) {
        super(name, args);
    }

    @Override
    public void checkSyntax() {
        if(args.size() != 1) {
            throw new IllegalArgumentException("wrong number of arguments for " + name + " command");
        }
    }

    @Override
    public String execute() {
        return RespEncoder.encode(
                InMemoryDatabase
                        .getInstance()
                        .getKeysMatchingPattern(convertGlobToRegex(args.get(1)))
        );
    }

    private String convertGlobToRegex(String glob) {
        return glob
                .replace("*", ".*")
                .replace(".", "\\.")
                .replace("?", ".");
    }

}
