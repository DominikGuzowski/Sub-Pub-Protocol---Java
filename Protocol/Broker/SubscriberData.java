package Protocol.Broker;

import java.net.InetAddress;
import java.util.ArrayList;

import Protocol.Connection.Connection;

/**
 * @author Dominik Guzowski, 19334866
 */

class SubscriberData {
    private Connection conn;
    public ArrayList<String> subscribedTopics;

    /**
     * <b><code>SubscriberData Constructor</b></code>. Takes in a connection to the
     * subscriber for future reference when sending content and also the first
     * subtopic that the subscriber has subscribed to under the main topic.
     * 
     * @param subscriberConn <b><code>Connection</b></code> to the subscribers
     *                       receiving port
     * @param topic          initial subtopics
     */
    public SubscriberData(Connection subscriberConn, String topic) {
        conn = subscriberConn;
        subscribedTopics = new ArrayList<String>();
        subscribedTopics.add(topic);
    }

    /**
     * Takes in a subtopic and if this subscriber doesn't already have it in its
     * list of subtopics, adds it to the list.
     * 
     * @param topic valid representation of a subtopic
     */
    public void addTopic(String topic) {
        if (!subscribedTopics.contains(topic))
            subscribedTopics.add(topic);
    }

    /**
     * Returns the <b><code>InetAddress</b></code> of the subscriber connection.
     * 
     * @return <b><code>InetAddress</b></code> ip address
     * @see InetAddress
     */
    public InetAddress getAddress() {
        return conn.getAddress();
    }

    /**
     * Returns the port of the subscriber connection.
     * 
     * @return <b><code>int</b></code> port
     */
    public int getPort() {
        return conn.getPort();
    }

    /**
     * Returns the <b><code>Connection</b></code> to the subscriber, which contains
     * the ip address and port.
     * 
     * @return <b><code>Connection</b></code> to the subscriber
     */
    public Connection getConnection() {
        return conn;
    }

    /**
     * Returns a copy of the ArrayList of subtopics that the subscriber has
     * subscribed to under the main topic.
     * 
     * @return <b><code>ArrayList<String></b></code> of subtopics the subscriber
     *         subscribed to
     */
    public ArrayList<String> getTopics() {
        return new ArrayList<String>(subscribedTopics);
    }

    /**
     * Returns a String representation of the subscriber.
     */
    public String toString() {
        return "Subscriber:[\u001B[1;33m" + getAddress().getHostAddress() + "\u001B[0m:\u001B[1;34m" + getPort()
                + "\u001B[0m]";
    }

    /**
     * Compares two subscribers based on their connections.
     */
    public boolean equals(SubscriberData sub) {
        return getAddress().equals(sub.getAddress()) && getPort() == sub.getPort();
    }

    /**
     * Hash function for the subscriber based on the String implementation of the
     * hashCode() function
     * 
     * @see String#hashCode()
     */
    public int hashCode() {
        return ("Subscriber$" + getAddress().getHostAddress() + ":" + getPort()).hashCode();
    }
}
