package Protocol;

/**
 * @author Dominik Guzowski, 19334866
 */

/**
 * This class contains information about the protocol, allowing to easily access certain bytes of the header by 
 * their names and also allows for comparing the byte values of the header to what they are expected to be by a given
 * component.
 */
public final class Protocol {
    // Header Layout: [PacketType, CacheReq, DataType, TopicLen, ...TopicBytes]
    public static final int  HEADER_LEN     = 4; // Length of the header bytes excluding the topic bytes.

    // PacketType
    public static final int  PACKET_TYPE    = 0; // Position in the header
    public static final byte BROKER         = (byte) 0x7F;
    public static final byte SUBSCRIBER     = (byte) 0x7A;
    public static final byte PUBLISHER      = (byte) 0x75;
    
    // CacheReq
    public static final int  CACHE_REQ      = 1; // Position in the header
    public static final byte CACHE_Y        = (byte) 0xA5;
    public static final byte CACHE_N        = (byte) 0x5A;
    
    // DataType
    public static final int  DATA_TYPE      = 2; // Position in the header
    public static final byte STR            = (byte) 0x20;
    public static final byte INT            = (byte) 0x30;
    public static final byte SUB            = (byte) 0x10;
    public static final byte UNSUB          = (byte) 0x11;
    public static final byte POS_ACK        = (byte) 0xAA;
    public static final byte NEG_ACK        = (byte) 0xA5;
    public static final byte BROKER_SUB     = (byte) 0xB0;
    public static final byte BROKER_UNSUB   = (byte) 0xB1;
    public static final byte BROKER_STR     = (byte) 0x2B;
    public static final byte BROKER_INT     = (byte) 0x3B;
    public static final byte TOPIC_QRY      = (byte) 0xB5; // Not used
    public static final byte TOPIC_OWN      = (byte) 0xBA;
    public static final byte TOPIC_RES      = (byte) 0xBF; // Not used
    
    public static final int TOPIC_LEN       = 3; // Position in the header

    /**
     * Shorthand helper function that is used in the protocol components to identify threads by their name.
     * @return current thread name
     */
    public static String ThreadName() {
        return Thread.currentThread().getName();
    }
}
