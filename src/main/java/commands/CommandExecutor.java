package commands;

import resp.RespEncoder;
import utils.ParsedCommand;


public final class CommandExecutor {
    public static String executeCommand(ParsedCommand parsedCommand) {
        if (parsedCommand == null) {
            return RespEncoder.encode(new IllegalArgumentException("Invalid command format"));
        }
        return switch (parsedCommand.name().toLowerCase()) {
            case "ping" -> new PingCommand(parsedCommand.name(), parsedCommand.args()).execute();
            case "echo" -> new EchoCommand(parsedCommand.name(), parsedCommand.args()).execute();
            case "get" -> new GetCommand(parsedCommand.name(), parsedCommand.args()).execute();
            case "set" -> new SetCommand(parsedCommand.name(), parsedCommand.args()).execute();
            default -> RespEncoder.encode(
                    new IllegalArgumentException("unknown command: '" + parsedCommand.name() + "'")
            );
        };
    }
}
