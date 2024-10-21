import core.RedisServer;
import core.RedisServer.ServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        try {
            ServerConfig serverConfig = new ServerConfig(6379, 1024, 5000);
            RedisServer server = new RedisServer(serverConfig);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutting down...");
                server.shutdown();
            }));
            server.start();
        } catch (Exception e) {
            log.error("Failed to start server: ", e);
            System.exit(1);
        }

    }
}
