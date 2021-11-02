package src;
import java.util.Scanner;

import Protocol.Connection.Connection;
import Protocol.Publisher.Publisher;
import Protocol.Subscriber.Subscriber;

/**
 * @author Dominik Guzowski, 19334866
 */

public class Dashboard {
    public static void main(String[] args) {
        try {
            int port = Integer.parseInt(args[0].replaceAll("[^0-9]", ""));
            String brokerIP = args[1];
            Connection local = new Connection(17133);
            Connection dest = new Connection(brokerIP, port);
            Subscriber sub = new Subscriber(local, dest);
            Publisher pub = new Publisher(dest, "instructions");
            Scanner s = new Scanner(System.in);
            String command = "";
            while (!command.equalsIgnoreCase("stop")) {
                System.out.println("Sub/Ins/Print/Unsub (s/i/p/u): ");
                command = s.nextLine();
                System.out.println();

                if (command.equalsIgnoreCase("s")) {

                    System.out.println("Enter topic: ");
                    String topic = s.nextLine();
                    while (topic.length() == 0) {
                        System.out.println("Enter topic: ");
                        topic = s.nextLine();
                    }
                    System.out.println("Request caching? (y/n): ");
                    String caching = s.nextLine();
                    boolean cache = caching.contains("y") || caching.contains("Y");
                    sub.subscribe(topic, cache);

                } else if (command.equalsIgnoreCase("i")) {
                    System.out.println("Enter topic: ");
                    String topic = s.nextLine();
                    while (topic.length() == 0) {
                        System.out.println("Enter topic: ");
                        topic = s.nextLine();
                    }

                    System.out.println("Enter instruction: ");
                    String content = s.nextLine();
                    while (content.length() == 0) {
                        System.out.println("Enter instruction: ");
                        content = s.nextLine();
                    }
                    pub.publish(topic, content, false);
                } else if (command.equalsIgnoreCase("stop")) {
                    break;
                } else if (command.equalsIgnoreCase("p")) {
                    sub.printCache();
                } else if (command.equalsIgnoreCase("clear-cache")) {
                    System.out.println("\u001B[1m  # Cache cleared.\u001B[0m");
                    sub.clearCache();
                } else if (command.equalsIgnoreCase("show")) {
                    System.out.println("\u001B[1m  # Showing notifs enabled.\u001B[0m");
                    sub.setNotifs(true);
                } else if (command.equalsIgnoreCase("hide")) {
                    System.out.println("\u001B[1m  # Showing notifs disabled.\u001B[0m");
                    sub.setNotifs(false);

                } else if (command.equalsIgnoreCase("u")) {
                    System.out.println("Enter topic: ");
                    String topic = s.nextLine();
                    while (topic.length() == 0) {
                        System.out.println("Enter topic: ");
                        topic = s.nextLine();
                    }
                    sub.unsubscribe(topic);
                }
            }
            s.close();
            sub.close();
            System.out.println("Program exited.");
            return;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
