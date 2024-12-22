package core;

import commands.CommandFactory;
import commands.RedisCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import replication.ReplicationManager;
import resp.RespEncoder;
import utils.ClientState;
import utils.ClientType;
import utils.ParsedCommand;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Set;

public final class CommandHandler {
    private static final Set<String> transactionalCommandNames = Set.of("multi", "exec", "discard");
    private static final Logger log = LoggerFactory.getLogger(CommandHandler.class);

    public static void handleCommand(ParsedCommand parsedCommand, ClientState state) {
        Deque<String> responseQueue = state.responseQueue();
        if(parsedCommand == null) {
            return;
        }
        try {
            RedisCommand command = CommandFactory.getCommand(parsedCommand, state.isInTransaction());
            try {
                command.checkSyntax();
                if (command.isReplicaCommand()) {
                    if (state.getClientType() != ClientType.REPLICA) {
                        state.setClientType(ClientType.REPLICA);
                        ReplicationManager.addReplicaClient(state);
                    }
                }
                if (command.isWriteCommand()) {
                    if (RedisServer.getReplicationInfo().getRole().equals("slave") && state.getClientType() != ClientType.MASTER) {
                        throw new IllegalArgumentException("Replica node cannot accept writes.");
                    }
                }
                if(!state.isInTransaction()) {
                    if(RedisServer.getReplicationInfo().getRole().equals("master")) {
                        if(command.isWriteCommand()) {
                            log.info("Write command received.");
                            ReplicationManager.propagateToReplicas(command);
                        }
                        responseQueue.offer(command.execute());
                        if(command.getName().equalsIgnoreCase("multi")) {
                            state.setInTransaction();
                        }
                    } else {
                        String executedResponse = command.execute();
                        if(!command.isWriteCommand()) {
                            responseQueue.offer(executedResponse);
                        }
                    }
                } else {
                    String commandName = command.getName().toLowerCase();
                    if(!transactionalCommandNames.contains(commandName)) {
                        state.transactionQueue().offer(command);
                        responseQueue.offer(RespEncoder.encode("QUEUED", true));
                    } else if(commandName.equals("multi")) {
                        responseQueue.offer(command.execute());
                    } else if(commandName.equals("discard")) {
                        responseQueue.offer(command.execute());
                        endTransaction(state);
                    } else if(commandName.equals("exec")) {
                        List<String> encodedCommands = new ArrayList<>();
                        while (!state.transactionQueue().isEmpty()) {
                            RedisCommand queuedCmd = state.transactionQueue().poll();
                            String txnResult = queuedCmd.execute();
                            encodedCommands.add(txnResult);
                            if (queuedCmd.isWriteCommand()
                                    && RedisServer.getReplicationInfo().getRole().equals("master")
                                    && state.getClientType() == ClientType.CLIENT) {
                                ReplicationManager.propagateToReplicas(queuedCmd);
                            }
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
