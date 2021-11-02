package src;

import Protocol.Broker.Broker;
import Protocol.Connection.Connection;

/**
 * @author Dominik Guzowski, 19334866
 */

public class BrokerServer {
    public static void main(String[] args) {
        // args[0] = this broker's port
        // args[1] = list of other brokers: 123.45.6.7:8910@123.45.6.7:8910@...

        int port = Integer.parseInt(args[0]);
        Connection[] brokers = getBrokers(args[1]);
       
        Broker broker;
        try {
            broker = new Broker(port);
        } catch(Exception e) {
            System.out.println("Error: Port already in use.");
            return;
        }
        for(Connection conn : brokers) {
            broker.addBroker(conn);
        }
        broker.listen();
        try {
            System.in.read();
            broker.stopListening();
        } catch(Exception e) {
            broker.stopListening();
        }
    }

    public static void waitFor(long millis) {
        long time = System.currentTimeMillis();
        while(System.currentTimeMillis() < time + millis);
    }

    public static Connection[] getBrokers(String list) {
        String[] addrs = list.split("@");
        Connection[] brokers = new Connection[addrs.length];
        int i = 0;
        for(String broker : addrs) {
            String[] ip_port = broker.split(":");
            if(ip_port.length != 2) {
                System.out.println("Invalid ip - port combination.");
                return new Connection[0];
            }
            if(ip_port[0].split("\\.").length != 4 && !ip_port[0].equalsIgnoreCase("localhost")) {
                System.out.println("Invalid ip address.");
                return new Connection[0];
            }
            if(Integer.parseInt(ip_port[1]) > 65535 || Integer.parseInt(ip_port[1]) < 1024) {
                System.out.println("Invalid port number.");
                return new Connection[0];
            }
            brokers[i++] = new Connection(ip_port[0], Integer.parseInt(ip_port[1]));
        }
        return brokers;
    }
}
