package commands;

import core.RedisServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resp.RespEncoder;

import java.util.List;

public class PSyncCommand extends RedisCommand {

    private static final Logger log = LoggerFactory.getLogger(PSyncCommand.class);

    public PSyncCommand(String name, List<String> args) {
        super(name, args);
    }

    @Override
    public String execute() {
        String response = "FULLRESYNC " +
                RedisServer.getReplicationInfo().getMasterReplId() +
                " " +
                RedisServer.getReplicationInfo().getMasterReplOffset();
        String encodedResponse = RespEncoder.encode(response, true);
        String encodedRdb = RedisServer.getInitialState();
        return encodedResponse + encodedRdb;
    }

    @Override
    public void checkSyntax() {
        if (getArgs().size() < 2) {
            throw new IllegalArgumentException("wrong number of arguments for " + name + " command");
        }
    }

    @Override
    public boolean isWriteCommand() {
        return false;
    }

    @Override
    public boolean isReplicaCommand() {
        return true;
    }
}
