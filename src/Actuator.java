package src;

import Protocol.Connection.Connection;
import Protocol.Publisher.Publisher;
import Protocol.Subscriber.Subscriber;

/**
 * @author Dominik Guzowski, 19334866
 */

public class Actuator {
    public static void main(String[] args) {
        try {
            int port = Integer.parseInt(args[0].replaceAll("[^0-9]", ""));
            int freq = Integer.parseInt(args[2].replaceAll("[^0-9]", ""));
            String brokerIP = args[3];
            Connection local = new Connection(52345);
            Connection dest = new Connection(brokerIP, port);
            Subscriber sub = new Subscriber(local, dest);
            Publisher pub = new Publisher(dest, args[1]);
            System.out.println("Receiving instructions from: cmd/" + args[1]);

            sub.subscribe("cmd/"+args[1], true);
            int offset = 0;
            int temp = 50;
            while (true) {
                temp = (int) (temp + (10 * (Math.random() - 0.5)) + offset);
                offset = 0;
                pub.publish(temp + "'C", true);
                String instruction = (String) sub.getRecent();
                if (instruction != null) {
                    if (instruction.contains("cool down")) {
                        int value = Integer.parseInt(instruction.substring(10, instruction.length()));
                        offset -= value;
                    } else if (instruction.contains("heat up")) {
                        int value = Integer.parseInt(instruction.substring(8, instruction.length()));
                        offset += value;
                    }
                    sub.clearRecent();
                }
                sleep(freq);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void sleep(long millis) {
        long time = System.currentTimeMillis();
        while (System.currentTimeMillis() < time + millis)
            ;
    }
}
