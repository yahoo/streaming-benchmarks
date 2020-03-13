/**
 *
 */

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.flink.api.java.utils.ParameterTool;

public class SocketInjector {
    final private static Logger log = Logger.getLogger(SocketInjector.class.getName());

    public static void main(String[] args) {

        // parse parameters
        ParameterTool params = ParameterTool.fromArgs(args);
        String redisHostName = params.getRequired("redis");
        int port = params.getInt("port", 9000);
        int throughput = params.getInt("throughput", 5000);

        EventGenerator generator = new EventGenerator(redisHostName);

        ServerSocketChannel serverSocketChannel;
        SocketChannel socketChannel;
        PrintWriter printWriter;

        try {
            log.info("Attempting to start connection");

            // open the connection
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.bind(new InetSocketAddress(port));

            socketChannel = serverSocketChannel.accept();
            printWriter = new PrintWriter(socketChannel.socket().getOutputStream(), true);

            log.info("Connected on port " + port);
            log.info("Starting generation at rate " + throughput + " events/sec");

            // generate events on socket by the specified rate
            RateLimiter rateLimiter = RateLimiter.create(throughput, 1, TimeUnit.SECONDS);

            while (true) {
                long eventsGenerated = 0L;

                while (eventsGenerated < throughput) {
                    rateLimiter.acquire();

                    String event  = generator.makeEventAt(System.currentTimeMillis());
                    printWriter.println(event);

                    eventsGenerated++;
                }

                log.info("Generated " + eventsGenerated + " in 1 second");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}