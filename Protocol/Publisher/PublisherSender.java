package Protocol.Publisher;

import java.net.DatagramPacket;
import java.net.DatagramSocket;

import Protocol.Connection.Connection;
import Protocol.Protocol;

/**
 * @author Dominik Guzowski, 19334866
 */

public class PublisherSender implements Runnable {

    private final int MAX_ATTEMPTS = 5;
    private final int ACK_TIMEOUT = 500;
    private final int MTU = 1500;

    private Connection destination;
    private String topic;
    private boolean cache;
    private Object nonSerializedContent;
    private byte[] content;
    private byte type;

    /**
     * <b><code>PublisherSender Constructor</code></b>. Takes in the topic to which
     * to publish, destination connection of the broker, <b><code>String</code></b>
     * or <b><code>Integer</code></b> content and whether to cache the published
     * content or not.
     * 
     * @param topic       to which to publish content
     * @param destination <b><code>Connection</code></b> to the broker
     * @param content     <b><code>String</code></b> or <b><code>Integer</code></b>
     *                    content
     * @param cache       true if caching requested, else false
     * @see #integerToMinimalByteArray(int)
     */
    public PublisherSender(String topic, Connection destination, Object content, boolean cache) {
        this.topic = topic;
        this.destination = destination;
        this.cache = cache;
        nonSerializedContent = content;
        if (content.getClass().getSimpleName().equals("String")) {
            this.content = ((String) content).getBytes();
            this.type = Protocol.STR;
        } else if (content.getClass().getSimpleName().equals("Integer")) {
            this.content = integerToMinimalByteArray(((Integer) content).intValue());
            this.type = Protocol.STR;
        } else {
            this.content = new byte[0];
            type = 0;
        }
    }

    /**
     * Takes in an integer and returns a byte array of necessary length to represent
     * the value of the integer, rather than always being 4 bytes.
     * 
     * @param value integer to be converted to a byte array
     * @return byte array representing the integer value in least bytes necessary
     */
    private byte[] integerToMinimalByteArray(int value) {
        int len = 4;
        for (int i = 0; i < 4; i++) {
            if ((value & (0xFF000000 >>> (8 * i))) == 0) {
                len--;
            } else
                break;
        }
        byte[] arr = new byte[len];
        for (int i = len - 1; i >= 0; i--) {
            arr[i] = (byte) (value >>> ((len - i - 1) * 8));
        }
        return arr;
    }

    /**
     * Takes in byte array buffer that was used to send the packet and the received
     * acknowledgement packet data bytes. Compares the headers of the two and checks
     * if appropriate bytes have been changed to represent an acknowledgement.
     * 
     * @param buffer with header and payload of the packet that was sent
     * @param ack    acknowledgement packet data bytes that were received in
     *               response to sending a publisher packet
     * @return <br>
     *         </br>
     *         <b><code>true</code></b> if the acknowledgement packet was valid <br>
     *         </br>
     *         <b><code>false</code></b> if the acknowledgement packet was not valid
     */
    public boolean verifyAcknowledgement(byte[] buffer, byte[] ack) {
        if (ack[Protocol.PACKET_TYPE] != Protocol.BROKER) {
            return false;
        }
        if (ack[Protocol.CACHE_REQ] != buffer[Protocol.CACHE_REQ]) {
            return false;
        }
        if (ack[Protocol.DATA_TYPE] != Protocol.POS_ACK && ack[Protocol.DATA_TYPE] != Protocol.NEG_ACK) {
            return false;
        }

        for (int i = 3; i < ack.length; i++) {
            if (buffer[i] != ack[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Thread run method. Assembles a packet and sends it on to the broker. Then
     * waits for an acknowledgement and if none is received or if the
     * acknowledgement is invalid, resends the packet. Continues to do so until a
     * valid acknowledgement is received or the maximum number of attempts is
     * exceeded.
     * 
     * @see Thread#run()
     */
    @Override
    public void run() {
        if (topic.length() > 255) {
            System.out.println(" [!] Topic length exceeds maximum length of 255 characters.");
            System.out.println(" [!] Publisher packet not sent.");
            return;
        }
        if (type == 0 || content.length == 0) {
            System.out.println(" [!] Invalid content type.");
            System.out.println(" [!] Publisher packet not sent.");
            return;
        }
        if (topic.length() + content.length > MTU) {
            System.out.println(" [!] Topic and content exceed maximum packet size of " + (MTU - Protocol.HEADER_LEN)
                    + " by " + (topic.length() + content.length - MTU - Protocol.HEADER_LEN) + " bytes.");
            System.out.println(" [!] Publisher packet not sent.");
            return;
        }
        byte[] header = assembleHeader();
        byte[] buffer = new byte[header.length + content.length];
        System.arraycopy(header, 0, buffer, 0, header.length);
        System.arraycopy(content, 0, buffer, header.length, content.length);
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length, destination.getAddress(),
                destination.getPort());
        try {
            DatagramSocket socket = new DatagramSocket();
            boolean ackReceived = false;
            int attempts = 0;
            while (!ackReceived && attempts < MAX_ATTEMPTS) {
                attempts++;
                byte[] ackBuffer = new byte[header.length];
                DatagramPacket ackPacket = new DatagramPacket(ackBuffer, ackBuffer.length);
                System.out.println(" $ Sending packet containing: " + nonSerializedContent);
                socket.send(packet);
                socket.setSoTimeout(ACK_TIMEOUT);
                try {
                    socket.receive(ackPacket);
                    ackReceived = verifyAcknowledgement(buffer, ackPacket.getData());
                    if (ackReceived) {
                        if(ackPacket.getData()[Protocol.DATA_TYPE] == Protocol.NEG_ACK) System.out.println(" > Ack received! Publishing failed. Attempt " + attempts + "/" + MAX_ATTEMPTS + ".");
                        else System.out.println(" > Ack received! Publishing successful. Attempt " + attempts + "/" + MAX_ATTEMPTS + ".");
                    }
                    else
                        System.out.println(" [!] Received invalid ack! Attempt " + attempts + "/" + MAX_ATTEMPTS + ".");
                } catch (Exception e) {
                    System.out.println(" [!] Didn't receive ack! Attempt " + attempts + "/" + MAX_ATTEMPTS + ".");
                }
            }
            socket.close();
        } catch (Exception e) {
            System.out.println(" [!] Socket error.");
            System.out.println(" [!] Publisher packet not sent.");
        }
    }

    /**
     * Puts together the header of the packet including the topic bytes.
     * 
     * @return header byte array including topic bytes
     */
    private byte[] assembleHeader() {
        byte[] header = new byte[(0xFF & Protocol.HEADER_LEN) + topic.length()];
        header[Protocol.PACKET_TYPE] = Protocol.PUBLISHER;
        header[Protocol.CACHE_REQ] = cache ? Protocol.CACHE_Y : Protocol.CACHE_N;
        header[Protocol.DATA_TYPE] = type;
        header[Protocol.TOPIC_LEN] = (byte) topic.length();
        System.arraycopy(topic.getBytes(), 0, header, Protocol.HEADER_LEN, topic.length());
        return header;
    }
}
