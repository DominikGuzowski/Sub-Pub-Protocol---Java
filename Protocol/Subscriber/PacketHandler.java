package Protocol.Subscriber;

import java.lang.Runnable;
import java.net.DatagramPacket;
import java.net.DatagramSocket;

import Protocol.Protocol;

/**
 * @author Dominik Guzowski, 19334866
 */

public class PacketHandler implements Runnable {

    private Subscriber subscriber;
    private DatagramPacket packet;

    /**
     * <code><b>PacketHandler Constructor</b></code>. Takes in the packet received
     * by the <code><b>SubscriberReceiver</b></code> and a reference to the parent
     * subscriber.
     * 
     * @param packet     DatagramPacket received by the
     *                   <code><b>SubscriberReceiver</b></code>
     * @param subscriber reference to the parent subscriber
     */
    public PacketHandler(DatagramPacket packet, Subscriber subscriber) {
        this.packet = packet;
        this.subscriber = subscriber;
    }

    @Override
    public void run() {
        sendAcknowledgement(Protocol.POS_ACK);
        byte[] data = unpack();
        String topic = getTopic(data);
        Object content;
        if (data[Protocol.DATA_TYPE] == Protocol.STR) {
            content = new String(getContent(data));
        } else if (data[Protocol.DATA_TYPE] == Protocol.INT) {
            content = integerContent(getContent(data));
        } else {
            System.out.println(" [!] Received unknown data type. Discarding packet.");
            return;
        }
        subscriber.addContent(topic, content);
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
        header[Protocol.PACKET_TYPE] = Protocol.SUBSCRIBER;
        header[Protocol.DATA_TYPE] = ackType;
        try {
            DatagramPacket p = new DatagramPacket(header, header.length, packet.getAddress(), packet.getPort());
            DatagramSocket socket = new DatagramSocket();
            socket.send(p);
            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
