package commands;

import utils.ParsedCommand;


public final class CommandFactory {
    public static RedisCommand getCommand(ParsedCommand parsedCommand, boolean inTransaction) {
        return switch (parsedCommand.name().toLowerCase()) {
            case "ping" -> new PingCommand(parsedCommand.name().toLowerCase(), parsedCommand.args());
            case "echo" -> new EchoCommand(parsedCommand.name().toLowerCase(), parsedCommand.args());
            case "get" -> new GetCommand(parsedCommand.name().toLowerCase(), parsedCommand.args());
            case "set" -> new SetCommand(parsedCommand.name().toLowerCase(), parsedCommand.args());
            case "incr" -> new IncrCommand(parsedCommand.name().toLowerCase(), parsedCommand.args());
            case "multi" -> new MultiCommand(parsedCommand.name().toLowerCase(), parsedCommand.args(), inTransaction);
            case "exec" -> new ExecCommand(parsedCommand.name().toLowerCase(), parsedCommand.args(), inTransaction);
            case "discard" -> new DiscardCommand(parsedCommand.name().toLowerCase(), parsedCommand.args(), inTransaction);
            default -> throw new UnsupportedOperationException("Unknown command: " + parsedCommand.name());
        };
    }
}
