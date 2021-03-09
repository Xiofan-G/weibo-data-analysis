package org.weibo.analysis.network;

import io.undertow.Undertow;
import io.undertow.server.handlers.resource.ClassPathResourceManager;
import io.undertow.websockets.core.AbstractReceiveListener;
import io.undertow.websockets.core.BufferedTextMessage;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.core.WebSockets;
import org.weibo.analysis.StreamingJob;

import java.util.ArrayList;

import static io.undertow.Handlers.*;

/**
 * bootstrapper for the backend activities, it initializes the web socket server which feeds the frontend the graph data,
 * also run the Flink job.
 */
public class Server {
    // list of the connected web socket clients
    private static ArrayList<WebSocketChannel> channels = new ArrayList<>();
    private static String webSocketListenPath = "/graphData";
    private static int webSocketListenPort = 8080;
    private static String webSocketHost = "0.0.0.0";

    public static void main(String[] args) throws Exception {
        System.out.println("=========flink job is starting==========");
        initWebSocketServer();
        initFlinkStream();
        System.out.println("=========flink job is running==========");

    }


    private static void initFlinkStream() throws Exception {
        StreamingJob.run();
    }

    private static void initWebSocketServer() {
        Undertow server = Undertow.builder().addHttpListener(webSocketListenPort, webSocketHost)
                .setHandler(path().addPrefixPath(webSocketListenPath, websocket((exchange, channel) -> {
                    channels.add(channel);
                    channel.getReceiveSetter().set(getListener());
                    channel.resumeReceives();
                })).addPrefixPath("/", resource(new ClassPathResourceManager(Server.class.getClassLoader(),
                        Server.class.getPackage())).addWelcomeFiles("index.html")))
                .build();
        server.start();
        System.out.println("=========Websocket is running==========");
    }


    private static AbstractReceiveListener getListener() {
        return new AbstractReceiveListener() {
            @Override
            protected void onFullTextMessage(WebSocketChannel channel, BufferedTextMessage message) {
                final String messageData = message.getData();
                for (WebSocketChannel session : channel.getPeerConnections()) {
                    WebSockets.sendText(messageData, session, null);
                }
            }
        };
    }


    public static void sendToAll(String message) {
        System.out.println(String.format("result sunk by websocket: %s", message));
        for (WebSocketChannel session : channels) {
            WebSockets.sendText(message, session, null);
        }
    }
}