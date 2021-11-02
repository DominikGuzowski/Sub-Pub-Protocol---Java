package Protocol.Subscriber;

import java.lang.Runnable;
import java.net.DatagramSocket;

import Protocol.Connection.Connection;

import java.net.DatagramPacket;

/**
 * @author Dominik Guzowski, 19334866
 */

class SubscriberReceiver implements Runnable {
    private final int MTU = 1500;
    private Connection local;
    private Subscriber subscriber;
    private boolean listen;
    private DatagramSocket socket;

    /**
     * <b><code>SubscriberReceiver Constructor</b></code>. Takes in a connection to
     * which brokers will be sending content packets. Also takes in a reference to
     * its parent subscriber.
     * 
     * @param local      <b><code>Connection</b></code> to the receiving port of the
     *                   subscriber
     * @param subscriber reference to the parent <b><code>Subscriber</b></code>
     */
    SubscriberReceiver(Connection local, Subscriber subscriber) {
        this.local = local;
        this.subscriber = subscriber;
    }

    /**
     * Thread run method. Runs indefinitely until stopped, listening for packets
     * from brokers, and instanciating handlers to deal with the packets
     * appropriately once received.
     * 
     * @see Thread#run()
     * @see PacketHandler
     */
    @Override
    public void run() {
        listen = true;
        try {
            System.out.println("\u001B[1m\nSubscriber receiver running...\u001B[0m");
            socket = new DatagramSocket(local.getPort(), local.getAddress());
            while (listen) {
                byte[] buffer = new byte[MTU];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                try {
                    socket.receive(packet);
                } catch (Exception e) {
                    continue;
                }
                Thread thread = new Thread(new PacketHandler(packet, subscriber));
                thread.start();
            }
            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Subscriber listener stopped.");
    }

    /**
     * Sets the listen boolean to false and forcibly attempts to close the listening
     * socket, causing an exception, which effectively causes the listener to stop
     * listening.
     */
    void stop() {
        listen = false;
        if (socket != null)
            socket.close();
    }
}
