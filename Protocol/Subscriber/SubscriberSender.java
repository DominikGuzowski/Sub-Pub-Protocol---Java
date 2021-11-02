package Protocol.Subscriber;

import java.net.DatagramPacket;
import java.net.DatagramSocket;

import Protocol.Connection.Connection;
import Protocol.Protocol;

/**
 * @author Dominik Guzowski, 19334866
 */

public class SubscriberSender {
    private final int MAX_ATTEMPTS = 5;
    private final int ACK_TIMEOUT = 500;
    private Connection connection;
    private Connection local;
    public final static byte BROKER = (byte) 0x7F;

    /**
     * <b><code>SubscriberSender Constructor</code></b>. Takes in the local
     * connection as well as the destination broker connection. Also takes a
     * reference to its parent subscriber.
     * 
     * @param connection <b><code>Connection</code></b> to the destination broker
     * @param local      <b><code>Connection</code></b> of the subscriber
     * @param subscriber reference to the parent Subscriber
     */
    public SubscriberSender(Connection connection, Connection local, Subscriber subscriber) {
        this.connection = connection;
        this.local = local;
    }

    /**
     * Subscribe function that sends a subscription packet to the specified broker
     * for the specified topic and information regarding whether the subscription is
     * to be cached or not. Caching means that the subscriber will be receiving
     * future content that is yet to be published and not caching works like a GET
     * Request and gets only currently cached content from the broker. <br>
     * </br>
     * Topic length may not exceed 255 characters.
     * 
     * @param topic that the subscriber wants to subscribe to
     * @param cache true if caching requested else false
     */
    public void subscribe(String topic, boolean cache) {
        if (topic.length() > 255) {
            System.out.println(" [!] Topic length exceeds maximum length of 255 characters.");
            System.out.println(" [!] Subscription not sent.");
            return;
        }

        byte[] header = assembleHeader(true, cache, topic.length());
        byte[] topicBytes = topic.getBytes();
        byte[] buffer = new byte[header.length + topicBytes.length];
        System.arraycopy(header, 0, buffer, 0, header.length);
        System.arraycopy(topicBytes, 0, buffer, header.length, topicBytes.length);
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length, connection.getAddress(),
                connection.getPort());
        try {
            DatagramSocket socket = new DatagramSocket(local.getPort() - 1, local.getAddress());
            boolean ackReceived = false;
            int attempts = 0;
            while (!ackReceived && attempts < MAX_ATTEMPTS) {
                attempts++;
                byte[] ackBuffer = new byte[buffer.length];
                DatagramPacket ackPacket = new DatagramPacket(ackBuffer, ackBuffer.length);
                System.out.println(" $ Sending subscription.");
                socket.send(packet);
                socket.setSoTimeout(ACK_TIMEOUT);
                try {
                    socket.receive(ackPacket);
                    ackReceived = verifyAcknowledgement(buffer, ackPacket);
                    if (ackReceived)
                        if (ackPacket.getData()[Protocol.DATA_TYPE] == Protocol.POS_ACK)
                            System.out.println(" > Ack Received! Subscription successful. Attempt " + attempts + "/"
                                    + MAX_ATTEMPTS + ".");
                        else
                            System.out.println(" > Ack Received! Subscription failed. Attempt " + attempts + "/"
                                    + MAX_ATTEMPTS + ".");
                    else
                        System.out.println(" [!] Received invalid ack! Attempt " + attempts + "/" + MAX_ATTEMPTS + ".");
                } catch (Exception e) {
                    System.out.println(" [!] Didn't receive ack! Attempt " + attempts + "/" + MAX_ATTEMPTS + ".");
                }
            }
            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(" [!] Socket error.");
            System.out.println(" [!] Subscription not sent.");
        }
    }

    /**
     * Unsubscribe function that sends an unsubscription packet to the broker for a
     * given topic, after which the broker will no longer be sending new content to
     * this subscriber. <br>
     * </br>
     * Topic length may not exceed 255 characters.
     * 
     * @param topic from which the subscriber wants to unsubscribe from
     */
    public void unsubscribe(String topic) {
        if (topic.length() > 255) {
            System.out.println(" [!] Topic length exceeds maximum length of 255 characters.");
            System.out.println(" [!] Unsubscription not sent.");
            return;
        }

        byte[] header = assembleHeader(false, false, topic.length());
        byte[] topicBytes = topic.getBytes();
        byte[] buffer = new byte[header.length + topicBytes.length];
        System.arraycopy(header, 0, buffer, 0, header.length);
        System.arraycopy(topicBytes, 0, buffer, header.length, topicBytes.length);
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length, connection.getAddress(),
                connection.getPort());
        try {
            DatagramSocket socket = new DatagramSocket(local.getPort() - 1, local.getAddress());
            boolean ackReceived = false;
            int attempts = 0;
            while (!ackReceived && attempts < MAX_ATTEMPTS) {
                attempts++;
                byte[] ackBuffer = new byte[buffer.length];
                DatagramPacket ackPacket = new DatagramPacket(ackBuffer, ackBuffer.length);
                System.out.println(" $ Sending unsubscription.");
                socket.send(packet);
                socket.setSoTimeout(ACK_TIMEOUT);
                try {
                    socket.receive(ackPacket);
                    ackReceived = verifyAcknowledgement(buffer, ackPacket);
                    if (ackReceived)
                        if (ackPacket.getData()[Protocol.DATA_TYPE] == Protocol.POS_ACK)
                            System.out.println(" > Ack Received! Unsubscription successful. Attempt " + attempts + "/"
                                    + MAX_ATTEMPTS + ".");
                        else
                            System.out.println(" > Ack Received! Unsubscription failed. Attempt " + attempts + "/"
                                    + MAX_ATTEMPTS + ".");
                    else
                        System.out.println(" [!] Received invalid ack! Attempt " + attempts + "/" + MAX_ATTEMPTS + ".");
                } catch (Exception e) {
                    System.out.println(" [!] Didn't receive ack! Attempt " + attempts + "/" + MAX_ATTEMPTS + ".");
                }
            }
            socket.close();
        } catch (Exception e) {
            System.out.println(" [!] Socket error.");
            System.out.println(" [!] Unubscription not sent.");
        }
    }

    /**
     * Puts together the header of the packet excluding the topic bytes.
     * 
     * @param sub   true if subscribing, false if unsubscribing
     * @param cache true if caching requested else false
     * @param len   length of the topic
     * @return header byte array
     */
    private byte[] assembleHeader(boolean sub, boolean cache, int len) {
        byte[] header = { Protocol.SUBSCRIBER, cache ? Protocol.CACHE_Y : Protocol.CACHE_N,
                sub ? Protocol.SUB : Protocol.UNSUB, (byte) len };
        return header;
    }

    /**
     * Takes in byte array buffer that was used to send the packet and the received
     * acknowledgement packet. Compares the headers of the two and checks if
     * appropriate bytes have been changed to represent an acknowledgement.
     * 
     * @param buffer    with header and payload of the packet that was sent
     * @param ackPacket acknowledgement packet that was received in response to
     *                  sending a content packet
     * @return <br>
     *         </br>
     *         <b><code>true</code></b> if the acknowledgement packet was valid <br>
     *         </br>
     *         <b><code>false</code></b> if the acknowledgement packet was not valid
     */
    private boolean verifyAcknowledgement(byte[] buffer, DatagramPacket ackPacket) {
        byte[] data = new byte[ackPacket.getLength()];
        System.arraycopy(ackPacket.getData(), 0, data, 0, data.length);

        if (data[Protocol.PACKET_TYPE] != Protocol.BROKER
                || (data[Protocol.DATA_TYPE] != Protocol.POS_ACK && data[Protocol.DATA_TYPE] != Protocol.NEG_ACK))
            return false;

        for (int i = 3; i < data.length; i++) {
            if (data[i] != buffer[i]) {
                return false;
            }
        }
        return true;
    }
}
