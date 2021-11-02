package Protocol.Broker;

import java.net.DatagramPacket;
import java.net.DatagramSocket;

import Protocol.Protocol;
import Protocol.Connection.Connection;

/**
 * @author Dominik Guzowski, 19334866
 */

class BrokerSender implements Runnable {
    public static final int ACK_TIMEOUT = 500;
    public static final int MAX_ATTEMPTS = 3;

    private byte type;
    private byte[] content;
    private byte[] topic;
    private Connection dest;
    private String name;
    private byte cache;

    /**
     * <b><code>BrokerSender Constructor</code></b>. Saves parameters and assigns a
     * random name to the thread. Parameters determine the header and payload of the
     * packet that will be sent once the thread runs.
     * 
     * @param topic    bytes representing the topic <b><code>String</code></b>
     * @param content  <b><code>String</code></b> or <b><code>Integer</code></b>
     *                 object to be sent as payload
     * @param conn     <b><code>Connection</code></b> to which the packet will be
     *                 sent
     * @param dataType <b><code>DATA_TYPE</code></b> of the packet to be sent
     * @see #integerToMinimalByteArray(int)
     */
    BrokerSender(byte[] topic, Object content, Connection conn, byte dataType) {
        name = "\u001B[33mSending Thread " + String.format("%04X", (int) (Math.random() * Short.MAX_VALUE))
                + ": \u001B[0m";
        this.topic = topic;
        dest = conn;
        type = dataType;
        if (content == null) {
            this.content = new byte[0];
        } else {
            if (dataType == Protocol.STR || dataType == Protocol.BROKER_STR) {
                this.content = ((String) content).getBytes();
            } else if (dataType == Protocol.INT || dataType == Protocol.TOPIC_OWN || dataType == Protocol.BROKER_INT) {
                this.content = integerToMinimalByteArray(((Integer) content).intValue());
            } else {
                this.content = new byte[0];
            }
        }
        cache = 0;
    }

    /**
     * <b><code>BrokerSender Constructor</code></b>. Saves parameters and assigns a
     * random name to the thread. Allows to specify whether caching is or isn't
     * requested. Content is passed directly as a byte array. Parameters determine
     * the header and payload of the packet that will be sent once the thread runs.
     * 
     * @param topic    bytes representing the topic <b><code>String</code></b>
     * @param content  byte array to be sent as payload
     * @param conn     <b><code>Connection</code></b> to which the packet will be
     *                 sent
     * @param dataType <b><code>DATA_TYPE</code></b> of the packet to be sent
     * @param cache    <br>
     *                 </br>
     *                 <b><code>CACHE_Y</code></b> if the packet requests caching
     *                 <br>
     *                 </br>
     *                 <b><code>CACHE_N</code></b> if the packet doesn't request
     *                 caching
     */
    BrokerSender(byte[] topic, byte[] content, Connection conn, byte dataType, byte cache) {
        name = "\u001B[33mSending Thread " + String.format("%04X", (int) (Math.random() * Short.MAX_VALUE))
                + ": \u001B[0m";
        this.topic = topic;
        this.content = content;
        dest = conn;
        type = dataType;
        this.cache = cache;
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
     * Thread run method. Assembles a packet and sends it on. Then waits for an
     * acknowledgement and if none is received or if the acknowledgement is invalid,
     * resends the packet. Continues to do so until a valid acknowledgement is
     * received or the maximum number of attempts is exceeded.
     * 
     * @see Thread#run()
     */
    @Override
    public void run() {
        Thread.currentThread().setName(name);
        System.out.println(Protocol.ThreadName() + "Sending content to " + dest + ".");
        try {
            byte[] buffer = assemblePacket();
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length, dest.getAddress(), dest.getPort());
            DatagramSocket socket = new DatagramSocket();
            boolean ackReceived = false;
            int attempts = 0;
            while (!ackReceived && attempts < MAX_ATTEMPTS) {
                attempts++;
                byte[] ackBuffer = new byte[topic.length + Protocol.HEADER_LEN];
                DatagramPacket ackPacket = new DatagramPacket(ackBuffer, ackBuffer.length);
                socket.send(packet);
                socket.setSoTimeout(ACK_TIMEOUT);
                try {
                    socket.receive(ackPacket);
                    ackReceived = verifyAcknowledgement(buffer, ackPacket);
                    if (!ackReceived)
                        System.out.println(Protocol.ThreadName() + "\u001B[31;1mReceived invalid ack! Attempt "
                                + attempts + "/" + MAX_ATTEMPTS + "\u001B[0m");
                    else
                        System.out.println(Protocol.ThreadName() + "\u001B[32;1mAck received! Attempt " + attempts + "/"
                                + MAX_ATTEMPTS + "\u001B[0m");
                } catch (Exception e) {
                    System.out.println(Protocol.ThreadName() + "\u001B[31;1mDidn't receive ack! Attempt " + attempts
                            + "/" + MAX_ATTEMPTS + "\u001B[0m");
                }
            }
            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(Protocol.ThreadName() + "Exiting...");
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

        if (data[Protocol.DATA_TYPE] != Protocol.POS_ACK && data[Protocol.DATA_TYPE] != Protocol.NEG_ACK)
            return false;
        for (int i = 3; i < data.length; i++) {
            if (data[i] != buffer[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Creates a valid protocol packet by combining the topic and content with
     * appropriate header information that was specified in the constructor.
     * 
     * @return byte array representing the packet header and content combined
     */
    private byte[] assemblePacket() {
        byte[] header = { Protocol.BROKER, cache != 0 ? cache : Protocol.CACHE_Y, type, (byte) topic.length };
        byte[] buffer = new byte[header.length + topic.length + content.length];
        System.arraycopy(header, 0, buffer, 0, header.length);
        System.arraycopy(topic, 0, buffer, header.length, topic.length);
        System.arraycopy(content, 0, buffer, header.length + topic.length, content.length);
        return buffer;
    }
}
