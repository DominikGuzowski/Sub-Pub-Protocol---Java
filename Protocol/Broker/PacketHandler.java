package Protocol.Broker;

import java.net.DatagramPacket;
import java.net.DatagramSocket;

import Protocol.Protocol;
import Protocol.Connection.Connection;

/**
 * @author Dominik Guzowski, 19334866
 */

class PacketHandler implements Runnable {
    private DatagramPacket packet;
    private Broker broker;
    private String name;

    /**
     * <b><code>PacketHandler Constructor</code></b>. Saves parameters and assigns a
     * randomly generated name to the thread.
     * 
     * @param packet <b><code>DatagramPacket</code></b> received by the
     *               <b><code>BROKER</code></b>
     * @param broker reference to this <b><code>BROKER</code></b>
     * @see Thread#Thread()
     * @see Broker#Broker(int)
     */
    public PacketHandler(DatagramPacket packet, Broker broker) {
        this.packet = packet;
        this.broker = broker;
        name = "\u001B[34mHandler Thread " + String.format("%04X", (int) (Math.random() * Short.MAX_VALUE))
                + ": \u001B[0m";
    }

    /**
     * Thread run method. Ran after the <b><code>BROKER</code></b> receives a
     * packet. Runs the appropriate handler depending on the
     * <b><code>PACKET_TYPE</code></b>.
     * 
     * @see #BrokerPacketHandler()
     * @see #SubscriberPacketHandler()
     * @see #PublisherPacketHandler()
     * @see Thread#run()
     */
    @Override
    public void run() {
        Thread.currentThread().setName(name);
        switch (packet.getData()[0]) {
            case Protocol.BROKER:
                BrokerPacketHandler();
                break;
            case Protocol.SUBSCRIBER:
                SubscriberPacketHandler();
                break;
            case Protocol.PUBLISHER:
                PublisherPacketHandler();
                break;
            default:
                System.out.println(
                        Protocol.ThreadName() + "\u001B[31;1m[!] Unknown Packet Type: \u001B[0mPacket discarded.");
                break;
        }
        System.out.println(Protocol.ThreadName() + "Exiting...");
    }

    /**
     * Uses the <b><code>DATA_TYPE</code></b> byte of the header (from the
     * <b><code>DatagramPacket</code></b> contents) to appropriately handle the
     * packet, or will drop the packet if the <b><code>DATA_TYPE</code></b> is
     * unknown. <br>
     * </br>
     * <b><code>TOPIC_OWN:</code></b> will assign a broker to a topic. <br>
     * </br>
     * <b><code>BROKER_SUB/BROKER_UNSUB:</code></b> will read the packet as a
     * subscriber packet. <br>
     * </br>
     * <b><code>BROKER_STR/BROKER_INT:</code></b> will read the packet as a
     * publisher packet.
     * 
     * @see #assignBrokerToTopic(String, byte[])
     * @see #resolveBrokerSubscriberPacket(String, byte[])
     * @see #resolveBrokerPublisherPacket(String, byte[])
     */
    private void BrokerPacketHandler() {
        System.out.println(Protocol.ThreadName() + "\u001B[32mHandling a Broker Packet!\u001B[0m");
        sendAcknowledgement(Protocol.POS_ACK);
        byte[] data = unpack();
        String topic = getTopic(data);
        switch (data[Protocol.DATA_TYPE]) {
            case Protocol.TOPIC_OWN:
                assignBrokerToTopic(topic, data);
                break;
            case Protocol.BROKER_SUB:
            case Protocol.BROKER_UNSUB:
                resolveBrokerSubscriberPacket(topic, data);
                break;
            case Protocol.BROKER_INT:
            case Protocol.BROKER_STR:
                resolveBrokerPublisherPacket(topic, data);
                break;
            default:
                System.out.println(Protocol.ThreadName() + "\u001B[31mUnhandled/Unknown Data Type.\u001B[0m");
        }

    }

    /**
     * Given a <b><code>BROKER</code></b> packet whose <b><code>DATA_TYPE</code></b>
     * is <b><code>BROKER_INT</code></b> or <b><code>BROKER_STR</code></b> (a
     * forwarded publisher packet), it resolves its contents based on header
     * information such as whether the content should be cached or not and under
     * what topic.
     * 
     * @param topic the topic from the header
     * @param data  the <b><code>DatagramPacket</code></b> contents, includes the
     *              protocol header and payload
     * @see #PublisherPacketHandler()
     */
    private void resolveBrokerPublisherPacket(String topic, byte[] data) {
        System.out.println(Protocol.ThreadName() + "Resolving forwarded publisher packet...");
        if (topic.contains("*"))
            return; // Publishers may not use the star operator
        Object cacheableContent = null;
        if (data[Protocol.DATA_TYPE] == Protocol.BROKER_STR) {
            String content = new String(getContent(data));
            cacheableContent = content;
            broker.sendContentToSubscribers(topic, content);
        } else if (data[Protocol.DATA_TYPE] == Protocol.BROKER_INT) {
            Integer content = integerContent(getContent(data));
            cacheableContent = content;
            broker.sendContentToSubscribers(topic, content);
        }
        if (data[Protocol.CACHE_REQ] == Protocol.CACHE_Y) {
            if (cacheableContent != null)
                broker.storeContent(topic, cacheableContent);
        }
    }

    /**
     * Given a <b><code>BROKER</code></b> packet whose <b><code>DATA_TYPE</code></b>
     * is <b><code>BROKER_SUB</code></b> or <b><code>BROKER_UNSUB</code></b> (a
     * forwarded subscriber packet), it resolves its contents based on header
     * information such as whether the subscription should be cached or not and
     * under what topic, and retrieves the subscriber ip address and port from the
     * payload.
     * 
     * @param topic the topic from the header
     * @param data  the <b><code>DatagramPacket</code></b> contents, includes the
     *              protocol header and payload (subscriber ip and port)
     * @see #SubscriberPacketHandler()
     */
    private void resolveBrokerSubscriberPacket(String topic, byte[] data) {
        System.out.println(Protocol.ThreadName() + "Resolving forwarded subscriber packet...");
        byte[] content = getContent(data);
        String address = retrieveAddress(content);
        int port = retrievePort(content);
        Connection subscriberConn = new Connection(address, port + 1);
        SubscriberData sub = broker.getSubscriber(topic, subscriberConn);

        if (data[Protocol.DATA_TYPE] == Protocol.BROKER_SUB) {
            broker.sendCachedDataToSubscriber(topic, subscriberConn);
            if (data[Protocol.CACHE_REQ] == Protocol.CACHE_Y) {
                if (sub != null) {
                    sub.addTopic(topic);
                    System.out.println(Protocol.ThreadName() + "Added subtopics '" + topic + "' to " + sub + ".");
                } else {
                    SubscriberData subData = new SubscriberData(subscriberConn, topic);
                    broker.addSubscriber(topic, subData);
                }
            }
        } else if (data[Protocol.DATA_TYPE] == Protocol.BROKER_UNSUB) {
            System.out.println(Protocol.ThreadName() + "Unsubscribing Subscriber:[" + subscriberConn + "] from topic '"
                    + topic + "'.");
            broker.unsubscribe(topic, subscriberConn);
        }
    }

    /**
     * Handles <b><code>SUBSCRIBER</code></b> packets, sending currently cached
     * content (if any) for the given topic in the header, caching the
     * <b><code>SUBSCRIBER</code></b> if requested for future content under the
     * given topic, and if no caching is requested, the currently existing cached
     * content is sent once (similarly to a GET Request). Unsubscribes the
     * <b><code>SUBSCRIBER</code></b> if requested, such that no further content
     * will be sent to that <b><code>SUBSCRIBER</code></b> in the future. The
     * subscriber packet contains no payload, the ip and port of the subscriber are
     * retrieved from the <b><code>DatagramPacket</code></b> header. <br>
     * </br>
     * If this <b><code>BROKER</code></b> doesn't currently own the topic that is
     * being subscribed/unsubscribed to, it checks if other brokers it knows own the
     * topic, and if none do then it acquires the topic and announces it to other
     * brokers.
     */
    private void SubscriberPacketHandler() {
        System.out.println(Protocol.ThreadName() + "\u001B[32mHandling a Subscriber Packet!\u001B[0m");
        byte[] data = unpack();
        String topic = getTopic(data);
        if(topic.startsWith("*")) {
            sendAcknowledgement(Protocol.NEG_ACK);
            System.out.println(Protocol.ThreadName() + "Attempted subscription without a main topic.");
            return; // Cannot subscribe without a main topic.
        }
        else sendAcknowledgement(Protocol.POS_ACK);

        boolean owned = checkTopicOwnership(topic);
        if (!owned) {
            broker.forwardSubscriberPacket(topic, data, new Connection(packet.getAddress(), packet.getPort()));
            return;
        }
        Connection subscriberConn = new Connection(packet.getAddress(), packet.getPort() + 1);
        SubscriberData sub = broker.getSubscriber(topic, subscriberConn);

        if (data[Protocol.DATA_TYPE] == Protocol.SUB) {
            broker.sendCachedDataToSubscriber(topic, subscriberConn);
            if (data[Protocol.CACHE_REQ] == Protocol.CACHE_Y) {
                if (sub != null) {
                    sub.addTopic(topic);
                    System.out.println(Protocol.ThreadName() + "Added subtopics '" + topic + "' to " + sub + ".");
                } else {
                    SubscriberData subData = new SubscriberData(subscriberConn, topic);
                    broker.addSubscriber(topic, subData);
                }
            }
        } else if (data[Protocol.DATA_TYPE] == Protocol.UNSUB) {
            System.out.println(Protocol.ThreadName() + "Unsubscribing Subscriber:[" + subscriberConn + "] from topic '"
                    + topic + "'.");
            broker.unsubscribe(topic, subscriberConn);
        }
    }

    /**
     * Handles <b><code>PUBLISHER</code></b> packets, sending the content to current
     * subscribers (if any) under the topic in the header caching the content if
     * requested for future subscribers under the given topic, and if no caching is
     * requested, only current subscribers will receive the content. <br>
     * </br>
     * If this <b><code>BROKER</code></b> doesn't currently own the topic that is
     * being subscribed/unsubscribed to, it checks if other brokers it knows own the
     * topic, and if none do then it acquires the topic and announces it to other
     * brokers.
     */
    private void PublisherPacketHandler() {
        System.out.println(Protocol.ThreadName() + "\u001B[32mHandling a Publisher Packet!\u001B[0m");
        
        byte[] data = unpack();
        String topic = getTopic(data);

        if (topic.contains("*"))
        {
            sendAcknowledgement(Protocol.NEG_ACK);
            System.out.println(Protocol.ThreadName() + "Attempted publishing with star operator.");
            return; // Publishers may not use the star operator
        }
        else sendAcknowledgement(Protocol.POS_ACK);


        boolean owned = checkTopicOwnership(topic);

        if (!owned) {
            broker.forwardPublisherPacket(topic, data, getContent(data));
            return;
        }


        Object cacheableContent = null;
        if (data[Protocol.DATA_TYPE] == Protocol.STR) {
            String content = new String(getContent(data));
            cacheableContent = content;
            broker.sendContentToSubscribers(topic, content);
        } else if (data[Protocol.DATA_TYPE] == Protocol.INT) {
            Integer content = integerContent(getContent(data));
            cacheableContent = content;
            broker.sendContentToSubscribers(topic, content);
        }
        if (data[Protocol.CACHE_REQ] == Protocol.CACHE_Y) {
            if (cacheableContent != null)
                broker.storeContent(topic, cacheableContent);
        }
    }

    /**
     * Assigns a <b><code>BROKER</code></b> to a topic when the said broker
     * announces their ownership of the topic.
     * 
     * @param topic the topic from the header
     * @param data  the <b><code>DatagramPacket</code></b> contents, includes the ip
     *              address and port of the broker who owns the topic
     */
    private void assignBrokerToTopic(String topic, byte[] data) {
        int port = retrievePort(getContent(data));
        String address = retrieveAddress(getContent(data));
        Connection conn = new Connection(address, port);
        broker.assignBrokerToTopic(topic, conn);
    }

    /**
     * Given a byte array, it converts the bytes to an integer where
     * <b><code>byte[0]</code></b> is the most significant byte of the integer.
     * 
     * @param intBytes input byte array whose value is to be changed into an integer
     * @return <b><code>Integer</code></b> value as computed from the byte array
     */
    private Integer integerContent(byte[] intBytes) {
        int value = 0;
        for (byte b : intBytes) {
            value <<= 8;
            value += 0xFF & b;
        }
        return (Integer) value;
    }

    /**
     * Given the contents of the <b><code>DatagramPacket</code></b> it returns the
     * payload without the header information.
     * 
     * @param data <b><code>DatagramPacket</code></b> contents, including the
     *             protocol header and payload.
     * @return byte array of the payload only.
     * @see #unpack()
     */
    private byte[] getContent(byte[] data) {
        int headerLength = (Protocol.HEADER_LEN + (0xFF & data[Protocol.TOPIC_LEN]));
        byte[] bytes = new byte[data.length - headerLength];
        System.arraycopy(data, headerLength, bytes, 0, bytes.length);
        return bytes;
    }

    /**
     * Sends an acknowledgement to the sender of the current packet. Regardless of
     * the <b><code>ackType</code></b>, an acknoweledgement confirms that the packet
     * was received.
     * 
     * @param ackType <br>
     *                </br>
     *                <b><code>POS_ACK</code></b> for a YES answer. <br>
     *                </br>
     *                <b><code>NEG_ACK</code></b> for a NO answer.
     */
    private void sendAcknowledgement(byte ackType) {
        byte[] header = new byte[Protocol.HEADER_LEN + (0xFF & packet.getData()[Protocol.TOPIC_LEN])];
        System.arraycopy(packet.getData(), 0, header, 0, header.length);
        header[Protocol.PACKET_TYPE] = Protocol.BROKER;
        header[Protocol.DATA_TYPE] = ackType;
        try {
            DatagramPacket p = new DatagramPacket(header, header.length, packet.getAddress(), packet.getPort());
            DatagramSocket socket = new DatagramSocket();
            socket.send(p);
            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(Protocol.ThreadName() + "Sent acknowledgement!");
    }

    /**
     * Returns a byte array of the contents of the
     * <b><code>DatagramPacket</code></b> without excess bytes.
     * 
     * @return byte arrays of <b><code>DatagramPacket</code></b> contents, includes
     *         the protocol header and payload (if any).
     */
    private byte[] unpack() {
        byte[] data = new byte[packet.getLength()];
        System.arraycopy(packet.getData(), 0, data, 0, data.length);
        return data;
    }

    /**
     * Given the data bytes from the <b><code>DatagramPacket</code></b>, it
     * retrieves the header topic bytes and returns the string version.
     * 
     * @param data <b><code>DatagramPacket</code></b> contents, includes the
     *             protocol header and payload (if any).
     * @return the topic as a <b><code>String</code></b>
     * @see #unpack()
     */
    private String getTopic(byte[] data) {
        byte[] topicBytes = new byte[0xFF & data[Protocol.TOPIC_LEN]];
        System.arraycopy(data, Protocol.TOPIC_LEN + 1, topicBytes, 0, topicBytes.length);
        return new String(topicBytes);
    }

    /**
     * Checks if the current broker owns the topic.
     * 
     * @param topic as a String (including main topic and subtopics (if any))
     * @return <b><code>true</code></b> if the broker owns the topic or no other
     *         broker that the current broker knows owns the topic <br>
     *         </br>
     *         <b><code>false</code></b> if another broker that the current broker
     *         knows owns the topic
     */
    private boolean checkTopicOwnership(String topic) {
        boolean owns = broker.hasSubscriberTopic(topic);
        if (owns)
            return true;
        owns = broker.hasCacheTopic(topic);
        if (owns)
            return true;
        boolean otherOwns = broker.checkTopicOwnership(topic);
        if (otherOwns)
            return false;
        broker.declareTopicOwnership(topic);
        return true;
    }

    /**
     * Given the payload portion of the <b><code>DatagramPacket</code></b> contents,
     * returns the first 4 bytes as the ip address.
     * 
     * @param content payload of the <b><code>DatagramPacket</code></b> contents,
     *                excluding the protocol header
     * @return ip address as a <b><code>String</code></b>, in the form
     *         <b>"A.B.C.D"</b>
     * @see #getContent(byte[])
     */
    private String retrieveAddress(byte[] content) {
        byte[] addressBytes = new byte[4];
        System.arraycopy(content, 0, addressBytes, 0, addressBytes.length);
        String address = "" + (addressBytes[0] & 0xFF);
        for (int i = 1; i < 4; i++) {
            address += "." + (addressBytes[i] & 0xFF);
        }
        return address;
    }

    /**
     * Given the payload portion of the <b><code>DatagramPacket</code></b> contents,
     * using byte[4] and byte[5], returns an integer representing the port.
     * 
     * @param content payload of the <b><code>DatagramPacket</code></b> contents,
     *                excluding the protocol header
     * @return <b><code>int</code></b> port
     * @see #getContent(byte[])
     */
    private int retrievePort(byte[] content) {
        return ((content[4] & 0xFF) << 8) + (content[5] & 0xFF);
    }
}
