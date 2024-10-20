import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
    public static void main(String[] args) {
        // You can use print statements as follows for debugging, they'll be visible when running tests.
        System.out.println("Logs from your program will appear here!");

        //  Uncomment this block to pass the first stage
        int port = 6379;
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            // Since the tester restarts your program quite often, setting SO_REUSEADDR
            // ensures that we don't run into 'Address already in use' errors
            serverSocket.setReuseAddress(true);
            // Wait for connection from client.
            while (true) {
                try (Socket clientSocket = serverSocket.accept()) {
                    OutputStream outputStream = clientSocket.getOutputStream();
                    BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    String line;
                    StringBuilder commandBuilder = new StringBuilder();

                    // Read and accumulate until we detect a complete command
                    while ((line = reader.readLine()) != null) {
                        System.out.println("Received command: " + line);
                        commandBuilder.append(line).append("\n");
                        if (commandBuilder.toString().contains("PING")) {
                            outputStream.write("+PONG\r\n".getBytes());
                            outputStream.flush();
                            commandBuilder.setLength(0);
                        }
                    }
                } catch (IOException e) {
                    System.out.println("IOException while handling client: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }
}
