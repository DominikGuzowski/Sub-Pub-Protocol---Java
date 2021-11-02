package Protocol.Broker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.Collectors;

import Protocol.Cache;
import Protocol.Protocol;
import Protocol.Connection.Connection;

/**
 * @author Dominik Guzowski, 19334866
 */

public class Broker {

    private HashMap<String, Connection> topicList;
    private Connection brokerConnection;
    private Cache<Object> cache;
    public Cache<SubscriberData> subscribers;
    private BrokerReceiver receiver;
    private ArrayList<Connection> brokers;

    /**
     * <b><code>Broker Constructor</b></code>. Takes in a port to indicate on which
     * port its listener will be receiving packets. Brokers always use the localhost
     * ip address.
     * 
     * @param port <b><code>int</b></code> port on which the
     *             <b><code>BrokerReceiver</b></code> will be listening for incoming
     *             packets
     * @throws Exception if the port specified is already in use
     */
    public Broker(int port) throws Exception {
        brokerConnection = new Connection(port);
        topicList = new HashMap<String, Connection>();
        subscribers = new Cache<SubscriberData>();
        subscribers.setMaxCacheLength(250000);
        cache = new Cache<Object>();
        cache.setMaxCacheLength(8);
        receiver = new BrokerReceiver(brokerConnection, this);
        brokers = new ArrayList<Connection>();
    }

    /**
     * Instantiates the listening thread of the broker, causing its receiver to
     * begin listening for incoming packets.
     * 
     * @see BrokerReceiver
     */
    public void listen() {
        Thread t = new Thread(receiver);
        t.setName("\u001B[35m<<Listener Thread>>: \u001B[0m");
        t.start();
    }

    /**
     * Checks if a different broker owns the specified topic.
     * 
     * @param topic String
     * @return <br>
     *         </br>
     *         <b><code>true</b></code> if the broker knows of a different broker
     *         who owns the specified topic <br>
     *         </br>
     *         <b><code>false</b></code> if the broker doesn't know of a different
     *         broker who owns the specified topic
     */
    boolean checkTopicOwnership(String topic) {
        return topicList.containsKey(getMainTopic(topic));
    }

    /**
     * Adds new content to the cache under the given topic/subtopic. If the number
     * of content instances in the cache under the given topic exceeds the maximum
     * cache size, the oldest content instance is removed.
     * 
     * @param topic   String
     * @param content <b><code>Integer</b></code> or <b><code>String</b></code>
     *                content provided by a publisher
     */
    void storeContent(String topic, Object content) {
        System.out.println(Protocol.ThreadName() + "Added content to '" + topic + "'.");
        cache.addContent(topic, content);
    }

    /**
     * Adds subscriber data to the list of subscribers under the main topic. A
     * subscriber may exist under many topics but only once per main topic.
     * 
     * @param topic      String of topic/subtopics from which the main topic is
     *                   extracted
     * @param subscriber Subscriber data including the connection to the subscriber
     *                   and a list of their subscribed subtopics
     */
    void addSubscriber(String topic, SubscriberData subscriber) {
        subscribers.addContent(getMainTopic(topic), subscriber);
        System.out.println(Protocol.ThreadName() + "Added new " + subscriber + " to topic '" + topic + "'.");
    }

    /**
     * Stores the connection to a broker in a HashMap where the key is the main
     * topic. Used to determine which broker in the system owns a given topic.
     * 
     * @param topic  main topic
     * @param broker <b><code>Connection</b></code> to the broker who declared
     *               ownership of the topic
     */
    void assignBrokerToTopic(String topic, Connection broker) {
        String mainTopic = getMainTopic(topic);
        if(!topicList.containsKey(mainTopic)) {
            System.out.println(Protocol.ThreadName() + "Assigning Broker:[" + broker + "] to topic '" + mainTopic + "'.");
            topicList.put(mainTopic, broker);
        }
    }

    /**
     * Adds a connection to a broker to the list of brokers. Allows this broker to
     * know what other brokers exist in the system for inter-broker communication.
     * <br>
     * </br>
     * All brokers should know of all other brokers in the system as the connections
     * between them <b><i>must</i></b> be a perfect graph. <br>
     * </br>
     * Must be used before listen() is called to avoid clashing in topic ownerships.
     * 
     * @param broker <b><code>Connection</b></code> to a different broker
     * @see #listen()
     */
    public void addBroker(Connection broker) {
        System.out.println(" $ Added broker [" + broker + "].");
        brokers.add(broker);
    }

    /**
     * Sends previously cached data (if any) to a new subscriber under the given
     * topic.
     * 
     * @param subscriptionTopics topic to which the subscriber subscribed to
     * @param subscriber         <b><code>Connection</b></code> to the subscriber
     */
    void sendCachedDataToSubscriber(String subscriptionTopics, Connection subscriber) {
        if (subscriptionTopics.endsWith("/**")) {
            String all = subscriptionTopics.substring(0, subscriptionTopics.length() - 1);
            sendCachedDataToSubscriber(all, subscriber);
            return;
        }
        System.out.println(Protocol.ThreadName() + "Checking for cached content...");
        HashMap<String, ArrayList<Object>> cachedData = cache.get(subscriptionTopics);
        for (String key : cachedData.keySet()) {
            byte[] topic = key.getBytes();
            if (cachedData.get(key) != null)
                for (Object o : cachedData.get(key)) {
                    System.out.println(
                            Protocol.ThreadName() + "Creating a sender to send cached '" + key + "' content...");
                    byte type = o.getClass().getSimpleName().equals("String") ? Protocol.STR : Protocol.INT;
                    BrokerSender sender = new BrokerSender(topic, o, subscriber, type);
                    Thread t = new Thread(sender);
                    t.start();
                }
        }
    }

    /**
     * Sends newly published content to existing subscribers (if any) of the given
     * topic.
     * 
     * @param topic   to which content is being published to
     * @param content <b><code>Integer</b></code> or <b><code>String</b></code>
     *                content published by a publisher
     */
    void sendContentToSubscribers(String topic, Object content) {
        String mainTopic = getMainTopic(topic);
        ArrayList<SubscriberData> subs = subscribers.get(mainTopic).get(mainTopic);
        ArrayList<SubscriberData> confirmedSubs = getConfirmedSubscribers(toArrayList(topic.split("/")), subs);
        byte[] topicBytes = topic.getBytes();
        for (SubscriberData sub : confirmedSubs) {
            System.out.println(Protocol.ThreadName() + "Creating sender to send new content...");
            byte type = content.getClass().getSimpleName().equals("String") ? Protocol.STR : Protocol.INT;
            BrokerSender sender = new BrokerSender(topicBytes, content, sub.getConnection(), type);
            Thread t = new Thread(sender);
            t.start();
        }
    }

    /**
     * Given a list of subtopics and a list of subscribers for the main topic,
     * returns a list of subscribers where at least one subscriber subtopic matched
     * the subtopic list.
     * 
     * @param topicList ArrayList of a split subtopic string
     * @param subs      ArrayList of subscribers for the main topic in question
     * @return ArrayList of subscribers where at least one of their subscribed
     *         subtopics matched the subtopic in question
     */
    ArrayList<SubscriberData> getConfirmedSubscribers(ArrayList<String> topicList, ArrayList<SubscriberData> subs) {
        if (subs == null)
            return new ArrayList<SubscriberData>();
        ArrayList<SubscriberData> confirmedSubs = new ArrayList<SubscriberData>();
        for (SubscriberData sub : subs) {
            ArrayList<String> topics = filterStartsWith(sub.getTopics(), topicList.get(0));
            for (String t : topics) {
                ArrayList<String> subTopics = toArrayList(t.split("/"));
                System.out.println(Protocol.ThreadName() + "Matching " + topicList + " " + subTopics);
                if (matchesTopic(topicList, subTopics)) {
                    confirmedSubs.add(sub);
                    break; // Avoids dupes
                }
            }
        }
        return confirmedSubs;
    }

    /**
     * Converts an array to an ArrayList.
     * 
     * @param <T>   type of the array
     * @param array to be converted
     * @return ArrayList equivalent of the array given
     */
    private <T> ArrayList<T> toArrayList(T[] array) {
        return new ArrayList<T>(Arrays.asList(array));
    }

    /**
     * Given a string that may contain subtopics, returns the main topic only.
     * 
     * @param topic string of topic/subtopics
     * @return <b><code>String</b></code> main topic
     */
    private String getMainTopic(String topic) {
        return topic.split("/")[0];
    }

    /**
     * Given a <b><code>String ArrayList</b></code> returns a new
     * <b><code>String ArrayList</b></code> containing only values which start with
     * the given string.
     * 
     * @param list       unfiltered ArrayList
     * @param startsWith String that will be used to filter wanted values
     * @return ArrayList containing only values which start with the given string
     */
    private ArrayList<String> filterStartsWith(ArrayList<String> list, String startsWith) {
        return new ArrayList<String>(list.stream().filter(s -> s.startsWith(startsWith)).collect(Collectors.toList()));
    }

    /**
     * Takes in two lists of split subtopics and compares them to check whether they
     * match, either directly or with the use of the * and ** operators. <br>
     * </br>
     * * means get all subtopics excluding the given topic. TopicName/* returns all
     * subtopics of TopicName but not TopicName itself. <br>
     * </br>
     * ** means get all subtopics including the given topic. TopicName/** returns
     * all subtopics of TopicName and TopicName itself.
     * 
     * @param topic    main topic list
     * @param subTopic subtopic list to be compared to the main list
     * @return <br>
     *         </br>
     *         <b><code>true</b></code> if the lists match <br>
     *         </br>
     *         <b><code>false</b></code> if the lists don't match
     */
    private boolean matchesTopic(ArrayList<String> topic, ArrayList<String> subTopic) {
        if (subTopic.size() - topic.size() > 1)
            return false;
        else if (subTopic.size() - topic.size() == 1 && !subTopic.get(subTopic.size() - 1).equals("*")
                && !subTopic.get(subTopic.size() - 1).equals("**"))
            return false;

        for (int i = 0; i < topic.size(); i++) {
            if (i < subTopic.size()) {
                if (!(topic.get(i).equals(subTopic.get(i)) || subTopic.get(i).equals("*")
                        || subTopic.get(i).equals("**"))) {
                    return false;
                }
            } else if (!subTopic.get(subTopic.size() - 1).equals("*")
                    && !subTopic.get(subTopic.size() - 1).equals("**")) {
                return false;
            } else {
                return true;
            }
        }
        if ((topic.size() >= subTopic.size() || subTopic.get(subTopic.size() - 1).equals("**")))
            return true;
        return false;
    }

    /**
     * Given a topic string and a connection to a subscriber, returns the existing
     * instance of the subscriber under the given main topic.
     * 
     * @param topic          String of topic/subtopics from which main topic is
     *                       extracted
     * @param subscriberConn <b><code>Connection</b></code> to the subscriber
     * @return existing instance of the subscriber under the given topic
     */
    SubscriberData getSubscriber(String topic, Connection subscriberConn) {
        String mainTopic = getMainTopic(topic);
        for (SubscriberData sub : getSubscribers(mainTopic)) {
            if (sub.getConnection().equals(subscriberConn)) {
                return sub;
            }
        }
        return null;
    }

    /**
     * Given the main topic, returns a list of all subscribers who have subscribed
     * to any subtopic under the main topic.
     * 
     * @param topic main topic
     * @return ArrayList of Subscriber Data that subscribed to anything under the
     *         main topic
     */
    ArrayList<SubscriberData> getSubscribers(String topic) {
        ArrayList<SubscriberData> subs = subscribers.get(topic).get(topic);
        return subs != null ? subs : new ArrayList<SubscriberData>();
    }

    /**
     * Given a specific topic/subtopic string and a connection to the subscriber,
     * removes the topic/subtopic from the subscriber's list of subtopics. If the
     * list is empty after unsubscribing, the subscriber is fully removed from the
     * main topic.
     * 
     * @param topic          specific topic/subtopic from which the subscriber wants
     *                       to unsubscribe from
     * @param subscriberConn <b><code>Connection</b></code> to the subscriber
     */
    void unsubscribe(String topic, Connection subscriberConn) {
        if (topic.equals("*")) {
            for (String t : subscribers.getTopics()) {
                unsubscribe(t + "/**", subscriberConn);
            }
            return;
        }
        SubscriberData sub = getSubscriber(topic, subscriberConn);
        if (sub == null)
            return;
        for (int i = 0; i < sub.subscribedTopics.size(); i++) {
            if (matchesTopic(toArrayList(sub.subscribedTopics.get(i).split("/")), toArrayList(topic.split("/")))) {
                System.out.println(Protocol.ThreadName() + "Matched \u001B[32;1m" + topic + "\u001B[0m to \u001B[34;1m"
                        + sub.subscribedTopics.get(i) + "\u001B[0m.");
                sub.subscribedTopics.set(i, null);
            }
        }
        sub.subscribedTopics.removeIf(s -> s == null);
        if (sub.subscribedTopics.size() == 0) {
            System.out.println(Protocol.ThreadName() + "Removing from [" + getMainTopic(topic) + "]: " + sub);
            subscribers.shallowRemove(getMainTopic(topic), sub);
        }
    }

    /**
     * Instructs the <b><code>BrokerReceiver</b></code> of this broker to stop
     * listening and hence stop receiving new packets.
     */
    public void stopListening() {
        receiver.stopListening();
    }

    /**
     * Returns a list of main topics that subscribers have subscribed to.
     * 
     * @return list of main topics of subscribers
     */
    ArrayList<String> getSubscriberTopics() {
        return subscribers.getTopics();
    }

    /**
     * Checks if the broker's list of subscribers contains the given main topic.
     * 
     * @param topic
     * @return <br>
     *         </br>
     *         <b><code>true</b></code> if the list of subscribers contains the main
     *         topic <br>
     *         </br>
     *         <b><code>false</b></code> if the list of subscribers doesn't contain
     *         the main topic
     * @see #getSubscriberTopics()
     */
    boolean hasSubscriberTopic(String topic) {
        return getSubscriberTopics().contains(getMainTopic(topic));
    }

    /**
     * Checks if the broker's content cache contains the given main topic.
     * 
     * @param topic
     * @return <br>
     *         </br>
     *         <b><code>true</b></code> if the cache contains the main topic <br>
     *         </br>
     *         <b><code>false</b></code> if the cache doesn't contain the main topic
     * @see #getCacheTopics()
     */
    boolean hasCacheTopic(String topic) {
        return getCacheTopics().contains(getMainTopic(topic));
    }

    /**
     * Returns a list of main topics that the cache contains.
     * 
     * @return list of main topics of the cached content
     */
    ArrayList<String> getCacheTopics() {
        return cache.getTopics();
    }

    /**
     * Given a topic/subtopic string that the broker doesn't already contain in
     * either the list of subscribers or the cache, and none of the known brokers in
     * the system own the topic already, the broker sends a topic ownership
     * declaration to all other brokers announcing that the given topic belongs to
     * this broker and all content relating to the topic should be forwarded to this
     * broker.
     * 
     * @param topic
     */
    void declareTopicOwnership(String topic) {
        String mainTopic = getMainTopic(topic);
        System.out.println(Protocol.ThreadName() + "Declaring ownership of topic '" + mainTopic + "'.");
        subscribers.addContent(topic, null);
        cache.addContent(topic, null);

        for (Connection broker : brokers) {
            BrokerSender sender = new BrokerSender(mainTopic.getBytes(), brokerConnection.getConnectionBytes(), broker,
                    Protocol.TOPIC_OWN, Protocol.CACHE_Y);
            Thread t = new Thread(sender);
            t.start();
        }
    }

    /**
     * Given the topic of the packet, header data of the packet and connection to a
     * subscriber, the broker converts the header to signify that the subscriber
     * packet has been forwarded and sends the subscription packet, containing the
     * subscriber ip address and receiving port in the payload, to a known broker
     * who owns the topic that the subscriber is subscribing to.
     * 
     * @param topic
     * @param data  header of the subscription packet (as they do not contain any
     *              payloads by default)
     * @param conn  <b><code>Connection</b></code> to the subscriber
     */
    void forwardSubscriberPacket(String topic, byte[] data, Connection conn) {
        System.out.println(Protocol.ThreadName() + "Forwarding subscriber packet...");
        byte type = data[Protocol.DATA_TYPE] == Protocol.SUB ? Protocol.BROKER_SUB
                : data[Protocol.DATA_TYPE] == Protocol.UNSUB ? Protocol.BROKER_UNSUB : 0;
        if (type == 0)
            return;

        Connection broker = topicList.get(getMainTopic(topic));
        byte[] address = conn.getAddress().getAddress();
        int subPort = conn.getPort();
        byte[] port = { (byte) ((subPort & 0xFF00) >> 8), (byte) (subPort & 0x00FF) };
        byte[] content = new byte[address.length + port.length];
        System.arraycopy(address, 0, content, 0, address.length);
        System.arraycopy(port, 0, content, address.length, port.length);
        BrokerSender sender = new BrokerSender(topic.getBytes(), content, broker, type, data[Protocol.CACHE_REQ]);
        Thread t = new Thread(sender);
        t.start();
    }

    /**
     * Given the topic of the packet, header data of the packet and payload content
     * of the packet, the broker converts the header to signify that the publisher
     * packet has been forwarded and sends the publisher packet, containing the
     * intact publisher payload, to a known broker who owns the topic that the
     * publisher is publishing to.
     * 
     * @param topic
     * @param data    header of the publisher packet and payload
     * @param content payload of the publisher packet
     */
    void forwardPublisherPacket(String topic, byte[] data, byte[] content) {
        System.out.println(Protocol.ThreadName() + "Forwarding publisher packet...");
        byte type = data[Protocol.DATA_TYPE] == Protocol.STR ? Protocol.BROKER_STR
                : data[Protocol.DATA_TYPE] == Protocol.INT ? Protocol.BROKER_INT : 0;
        if (type == 0)
            return;
        Connection broker = topicList.get(getMainTopic(topic));
        BrokerSender sender = new BrokerSender(topic.getBytes(), content, broker, type, data[Protocol.CACHE_REQ]);
        Thread t = new Thread(sender);
        t.start();
    }
}
