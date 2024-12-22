package commands;

import db.InMemoryDatabase;
import resp.RespEncoder;

import java.util.List;
import java.util.regex.Pattern;

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
    public boolean isWriteCommand() {
        return false;
    }

    @Override
    public boolean isReplicaCommand() {
        return false;
    }

    @Override
    public String execute() {
        return RespEncoder.encode(
                InMemoryDatabase
                        .getInstance()
                        .getKeysMatchingPattern(convertGlobToRegex(args.getFirst()))
        );
    }

    private String convertGlobToRegex(String glob) {
        if (glob == null) {
            return "";
        }
        StringBuilder regex = new StringBuilder("^");
        boolean escaping = false;
        for (int i = 0; i < glob.length(); i++) {
            char c = glob.charAt(i);
            if (escaping) {
                regex.append(Pattern.quote(String.valueOf(c)));
                escaping = false;
                continue;
            }
            switch (c) {
                case '\\' -> escaping = true;
                case '*' -> regex.append(".*");
                case '?' -> regex.append(".");
                case '[' -> {
                    regex.append('[');
                    while (i < glob.length() - 1) {
                        i++;
                        c = glob.charAt(i);
                        regex.append(c);
                        if (c == ']') {
                            break;
                        }
                    }
                }
                case ']', '^', '$', '.', '{', '}', '+', '(' -> regex.append('\\').append(c);
                default -> regex.append(c);
            }
        }
        regex.append("$");
        return regex.toString();
    }
}
