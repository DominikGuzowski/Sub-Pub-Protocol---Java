package Protocol.Connection;

import java.net.DatagramSocket;
import java.net.InetAddress;

/**
 * @author Dominik Guzowski, 19334866
 */

public class Connection {

    private InetAddress address;
    private int port;

    /**
     * <code><b>Connection Constructor</b></code>. Given the port, saves the
     * connection information, where the ip address is localhost. Tests whether the
     * port is available and throws an exception if it is already in use.
     * 
     * @param port
     * @throws Exception if port is already in use on localhost
     */
    public Connection(int port) throws Exception {
        this.port = port;
        address = InetAddress.getLocalHost();
        DatagramSocket test = new DatagramSocket(port, address);
        test.close();
    }

    /**
     * <code><b>Connection Constructor</b></code>. Takes in an ip address in the
     * form of a byte array and a port and saves them for future connections.
     * 
     * @param address byte array of length 4 representing the ip address
     * @param port
     * @throws Exception if the address byte array is invalid
     */
    public Connection(byte[] address, int port) throws Exception {
        this.port = port;
        this.address = InetAddress.getByAddress(address);
    }

    /**
     * <code><b>Connection Constructor</b></code>. Takes in the ip address as a
     * string and the port and saves them for future connections. If the address
     * string is invalid, the address will be null and an exception message will be
     * printed. Use with caution.
     * 
     * @param address String containing the address
     * @param port
     */
    public Connection(String address, int port) {
        try {
            this.port = port;
            if (address.equalsIgnoreCase("localhost"))
                this.address = InetAddress.getLocalHost();
            else
                this.address = InetAddress.getByName(address);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * <code><b>Connection Constructor</b></code>. Takes in the InetAddress ip
     * address and the port and saves them for future connections.
     * 
     * @param address InetAddress
     * @param port
     */
    public Connection(InetAddress address, int port) {
        this.address = address;
        this.port = port;
    }

    /**
     * Returns the InetAddress of the connection.
     * 
     * @return InetAddress
     */
    public InetAddress getAddress() {
        return address;
    }

    /**
     * Returns the port of the connection.
     * 
     * @return port
     */
    public int getPort() {
        return port;
    }

    /**
     * <code><b>Connection</b></code> implementation of the toString() function.
     */
    @Override
    public String toString() {
        return "\u001B[1;33m" + address.getHostAddress() + "\u001B[0m:\u001B[1;34m" + port + "\u001B[0m";
    }

    /**
     * Returns a byte array of size 6, where the first 4 bytes represent the ip
     * address and the last 2 bytes represent the port number, where the least
     * significant byte of the port is its second byte
     * 
     * @return byte array containing both ip address and port
     */
    public byte[] getConnectionBytes() {
        byte[] conn = new byte[6];
        System.arraycopy(getAddress().getAddress(), 0, conn, 0, 4);
        byte[] port = { (byte) ((getPort() & 0xFF00) >> 8), (byte) (getPort() & 0x00FF) };
        System.arraycopy(port, 0, conn, 4, 2);
        return conn;
    }

    public boolean equals(Connection conn) {
        return port == conn.getPort() && address.equals(conn.getAddress());
    }
}
