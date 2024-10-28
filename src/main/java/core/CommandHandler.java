package core;

import commands.CommandFactory;
import commands.RedisCommand;
import resp.RespEncoder;
import utils.ClientState;
import utils.ParsedCommand;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Set;

public final class CommandHandler {
    private static final Set<String> transactionalCommandNames = Set.of("multi", "exec", "discard");

    public static void handleCommand(ParsedCommand parsedCommand, ClientState state) {
        Deque<String> responseQueue = state.responseQueue();
        if(parsedCommand == null) {
            return;
        }
        try {
            RedisCommand command = CommandFactory.getCommand(parsedCommand, state.isInTransaction());
            try {
                command.checkSyntax();
                if(!state.isInTransaction()) {
                    responseQueue.offer(command.execute());
                    if(command.getName().equalsIgnoreCase("multi")) {
                        state.setInTransaction();
                    }
                } else {
                    if(!transactionalCommandNames.contains(command.getName().toLowerCase())) {
                        state.transactionQueue().offer(command);
                        responseQueue.offer(RespEncoder.encode("QUEUED", true));
                    } else if(command.getName().equalsIgnoreCase("multi")) {
                        responseQueue.offer(command.execute());
                    } else if(command.getName().equalsIgnoreCase("discard")) {
                        responseQueue.offer(command.execute());
                        endTransaction(state);
                    } else if(command.getName().equalsIgnoreCase("exec")) {
                        List<String> encodedCommands = new ArrayList<>();
                        while(!state.transactionQueue().isEmpty()) {
                            encodedCommands.add(state.transactionQueue().poll().execute());
                        }
                        responseQueue.offer(RespEncoder.encodeTransaction(encodedCommands));
                        endTransaction(state);
                    }
                }
            } catch (IllegalArgumentException e) {
                responseQueue.offer(RespEncoder.encode(e));
                if(state.isInTransaction() && !transactionalCommandNames.contains(command.getName().toLowerCase())) {
                    endTransaction(state);
                }
            }
        } catch(RuntimeException e) {
            responseQueue.offer(RespEncoder.encode(e));
        }
    }

    private static void endTransaction(ClientState state) {
        state.endTransaction();
    }

}
