package commands;

import java.util.List;

public abstract class RedisCommand {

    protected final String name;
    protected final List<String> args;

    public RedisCommand(String name, List<String> args) {
        this.name = name;
        this.args = args;
    }

    public String getName() {
        return name;
    }
    public List<String> getArgs() {return args;}

    public abstract String execute();
    public abstract void checkSyntax();
    public abstract boolean isWriteCommand();
    public abstract boolean isReplicaCommand();
}
