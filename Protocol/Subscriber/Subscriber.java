package Protocol.Subscriber;

import java.util.ArrayList;
import java.util.HashMap;

import Protocol.Cache;
import Protocol.Connection.Connection;

/**
 * @author Dominik Guzowski, 19334866
 */

public class Subscriber {

    private SubscriberSender sender;

    private Cache<Object> cache;
    private SubscriberReceiver receiver;
    private Thread receiverThread;
    private boolean showNotif;
    private Object recentMessage;

    /**
     * <b><code>Subscriber Constructor</code></b>. Takes in a local connection to
     * which the broker will be sending content and a destination connection to the
     * broker's receiver. Starts the receiver for receiving packets immediately.
     * 
     * @param localConnection       connection to the receiver
     * @param destinationConnection connection to the broker
     */
    public Subscriber(Connection localConnection, Connection destinationConnection) {
        cache = new Cache<Object>();
        receiver = new SubscriberReceiver(localConnection, this);
        sender = new SubscriberSender(destinationConnection, localConnection, this);
        receiverThread = new Thread(receiver);
        receiverThread.start();
        showNotif = true;
        recentMessage = null;
    }

    /**
     * Forces the receiver to stop listening for new broker packets and closes the
     * socket.
     */
    public void close() {
        receiver.stop();
    }

    /**
     * Given a topic and whether the subscription is to be cached and sends a
     * subscription packet to the broker. Not requesting caching will result in a
     * GET request, receiving only currently cached content from the broker, while
     * caching will cause the broker to keep sending newly published content until
     * unsubscribed.
     * 
     * @param topic to which the subscriber is subscribing
     * @param cache true if caching requested else false
     */
    public void subscribe(String topic, boolean cache) {
        sender.subscribe(topic, cache);
    }

    /**
     * Given the topic, sends an unsubscription packet to the broker, after which no
     * more newly published content will be sent to this subscriber for that given
     * topic.
     * 
     * @param topic from which to unsubscribe from
     */
    public void unsubscribe(String topic) {
        sender.unsubscribe(topic);
    }

    /**
     * After receiving a content packet from the broker, adds the content to this
     * subscriber's cache. If notifications are enabled, will print < ! > every time
     * new content is added.
     * 
     * @param topic   of the content
     * @param content <b><code>String</code></b> or <b><code>Integer</code></b>
     *                content received from the broker
     */
    void addContent(String topic, Object content) {
        cache.addContent(topic, content);
        recentMessage = content;
        if (showNotif)
            System.out.print("<!>");
    }

    /**
     * Prints the current state of this subscriber's cache.
     */
    public void printCache() {
        HashMap<String, ArrayList<Object>> list = cache.get("*");
        for (String topic : list.keySet()) {
            System.out.println("Topic: " + topic);
            int count = 1;
            for (Object obj : list.get(topic)) {
                System.out.println(count++ + ": " + obj);
            }
        }
    }

    /**
     * Enables/Disables the printing of notifications every time new content is
     * received.
     * 
     * @param notif true to enable, false to disable
     */
    public void setNotifs(boolean notif) {
        showNotif = notif;
    }

    /**
     * Returns the most recent content received by this subscriber.
     * 
     * @return most recent content
     */
    public Object getRecent() {
        return recentMessage;
    }

    /**
     * Clears most recent content.
     */
    public void clearRecent() {
        recentMessage = null;
    }

    /**
     * Completely clears this subscriber's cache.
     */
    public void clearCache() {
        cache = new Cache<Object>();
    }

    /**
     * Returns a HashMap containing the topics and content received by this
     * subscriber while also clearing the cache.
     * 
     * @return all topics and content received by the subscriber
     */
    public HashMap<String, ArrayList<Object>> flush() {
        HashMap<String, ArrayList<Object>> content = cache.get("*");
        clearCache();
        return content;
    }
}
