import core.RedisServer;
import core.RedisServer.ServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);
    private static final int DEFAULT_PORT = 6379;
    private static final int BUFFER_SIZE = 1024;
    private static final int DEFAULT_TIMEOUT = 5000;

    public static void main(String[] args) {
        try {
            ServerConfig config = parseConfig(args);
            startServer(config);
        } catch (Exception e) {
            log.error("Failed to start server: ", e);
            System.exit(1);
        }
    }

    private static ServerConfig parseConfig(String[] args) {
        Map<String, String> properties = new HashMap<>();

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port":
                    if (i + 1 < args.length) {
                        int port = parsePort(args[++i]);
                        properties.put("port", String.valueOf(port));
                    }
                    break;
                case "--dir":
                    if (i + 1 < args.length) {
                        properties.put("dir", args[++i]);
                    }
                    break;
                case "--dbfilename":
                    if (i + 1 < args.length) {
                        properties.put("dbfilename", args[++i]);
                    }
                    break;
                case "--replicaof":
                    if(i + 1 < args.length) {
                        properties.put("replicaof", args[++i]);
                    }
                    break;
            }
        }

        return new ServerConfig(
                Integer.parseInt(properties.getOrDefault("port", String.valueOf(DEFAULT_PORT))),
                BUFFER_SIZE,
                DEFAULT_TIMEOUT,
                properties
        );
    }

    private static int parsePort(String portStr) {
        int port = Integer.parseInt(portStr);
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535");
        }
        return port;
    }

    private static void startServer(ServerConfig config) throws IOException {
        RedisServer server = new RedisServer(config);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down...");
            server.shutdown();
        }));

        server.start();
    }
}