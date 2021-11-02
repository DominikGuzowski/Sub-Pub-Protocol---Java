package Protocol.Broker;

import java.net.DatagramPacket;
import java.net.DatagramSocket;

import Protocol.Protocol;
import Protocol.Connection.Connection;

/**
 * @author Dominik Guzowski, 19334866
 */

class BrokerReceiver implements Runnable {

    private Broker broker;
    private Connection receivingConnection;
    private boolean listen;
    private DatagramSocket socket;

    /**
     * <b><code>BrokerReceiver Constructor</b></code>. Takes in a connection to
     * which subscribers, publisher and other brokers will send packets. Also takes
     * in a reference to its parent broker.
     * 
     * @param brokerConnection <b><code>Connection</b></code> to the receiving port
     *                         of the broker
     * @param newBroker        reference to the parent <b><code>Broker</b></code>
     */
    BrokerReceiver(Connection brokerConnection, Broker newBroker) {
        broker = newBroker;
        receivingConnection = brokerConnection;
        listen = true;
    }

    /**
     * Thread run method. Runs indefinitely until stopped, listening for packets
     * from subscribers, publishers and other brokers, and instanciating handlers to
     * deal with the packets appropriately once received.
     * 
     * @see Thread#run()
     * @see PacketHandler
     */
    @Override
    public void run() {
        try {
            socket = new DatagramSocket(receivingConnection.getPort(), receivingConnection.getAddress());
            System.out.println();
            System.out.println(Protocol.ThreadName() + "\u001B[1;31m[!] \u001B[0mThe broker is listening on ["
                    + receivingConnection + "\u001B[0m]...\n");
            while (listen) {
                byte[] buffer = new byte[1500];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                try {
                    System.out.println(Protocol.ThreadName() + "Ready to receive a packet.");
                    socket.receive(packet);
                    System.out.println("\n" + Protocol.ThreadName() + "Received Packet. Instanciating a handler...");
                    new Thread(new PacketHandler(packet, broker)).start();
                } catch (Exception e) {
                    System.out.println(Protocol.ThreadName() + "Broker listener is stopping...");
                }
            }
            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(Protocol.ThreadName() + "\u001B[1;31m[!] \u001B[0mThe broker is no longer listening on ["
                + receivingConnection + "\u001B[0m]!\n");
    }

    /**
     * Sets the listen boolean to false and forcibly attempts to close the listening
     * socket, causing an exception, which effectively causes the listener to stop
     * listening.
     */
    public void stopListening() {
        listen = false;
        if (socket != null)
            socket.close();
    }

}
