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
                Socket clientSocket = serverSocket.accept();
                try (InputStreamReader inputStreamReader = new InputStreamReader(clientSocket.getInputStream());
                     BufferedReader reader = new BufferedReader(inputStreamReader);
                     OutputStream outputStream = clientSocket.getOutputStream()) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (line.isEmpty() || line.equals("\n")) {
                            continue;
                        }
                        String response = "+PONG\r\n";
                        System.out.println(response);
                        outputStream.write(response.getBytes());
                        outputStream.flush();
                    }
                } catch (IOException e) {
                    System.out.println("IOException: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }
}
