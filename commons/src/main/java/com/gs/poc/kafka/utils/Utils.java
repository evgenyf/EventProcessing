package com.gs.poc.kafka.utils;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Utils {

    public static File tempDir(String name) {
        String ioDir = System.getProperty("java.io.tmpdir");
        return new File(ioDir, "kafka-"+name);
    }

    public static List<Integer> choosePorts(int count) throws IOException {
        List<Integer> ports = new ArrayList(count);
        List<ServerSocket> sockets = new ArrayList(count);
        for (int i = 0; i < count; i++) {
            try {
                ServerSocket socket = new ServerSocket(0);

                sockets.add(socket);
            } catch (IOException e) {
                throw new IOException("No available socket");
            }
        }
        for (ServerSocket socket: sockets){
            ports.add(socket.getLocalPort());
            socket.close();
        }
        return ports;
    }

    public static Properties toProperties(Map<Object, Object> map){
        Properties result = new Properties();
        result.putAll(map);
        return result;
    }
}
