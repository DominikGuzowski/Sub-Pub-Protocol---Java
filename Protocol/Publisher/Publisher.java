package Protocol.Publisher;

import Protocol.Connection.Connection;

/**
 * @author Dominik Guzowski, 19334866
 */

public class Publisher {

    private String topic;

    private Connection destination;

    /**
     * <b><code>Publisher Constructor</code></b>. Takes in the connection to the
     * broker as well as the main topic that the publisher will be publishing to.
     * 
     * @param destination <b><code>Connection</code></b> to the broker
     * @param topic       main topic for publishing
     */
    public Publisher(Connection destination, String topic) {
        this.destination = destination;
        this.topic = topic;
    }

    /**
     * Given a new custom topic, string content and whether to cache the content or
     * not, sends a publisher packet to the broker. If caching is requested the
     * content will be saved by the broker, else it will be sent only to current
     * subscribers. Not caching can be used for sending instructions.
     * <br></br>
     * Topic may not exceed 255 characters. Topic along with the content may not exceed 1496 bytes.
     * @param topic   custom topic to which to publish content
     * @param content string content
     * @param cache   true if caching requested, else false.
     */
    public void publish(String topic, String content, boolean cache) {
        PublisherSender sender = new PublisherSender(topic, destination, content, cache);
        Thread t = new Thread(sender);
        t.start();
    }

    /**
     * Given a new custom topic, integer content and whether to cache the content or
     * not, sends a publisher packet to the broker. If caching is requested the
     * content will be saved by the broker, else it will be sent only to current
     * subscribers. Not caching can be used for sending instructions.
     * <br></br>
     * Topic may not exceed 255 characters. Topic along with the content may not exceed 1496 bytes.
     * @param topic   custom topic to which to publish content
     * @param content integer content
     * @param cache   true if caching requested, else false.
     */
    public void publish(String topic, Integer content, boolean cache) {
        PublisherSender sender = new PublisherSender(topic, destination, content, cache);
        Thread t = new Thread(sender);
        t.start();
    }

    /**
     * Given String content and whether to cache the content or
     * not, sends a publisher packet to the broker. If caching is requested the
     * content will be saved by the broker, else it will be sent only to current
     * subscribers. Not caching can be used for sending instructions.
     * <br></br>
     * Topic may not exceed 255 characters. Topic along with the content may not exceed 1496 bytes.
     * @param content String content
     * @param cache   true if caching requested, else false.
     */
    public void publish(String content, boolean cache) {
        PublisherSender sender = new PublisherSender(topic, destination, content, cache);
        Thread t = new Thread(sender);
        t.start();
    }

    /**
     * Given integer content and whether to cache the content or
     * not, sends a publisher packet to the broker. If caching is requested the
     * content will be saved by the broker, else it will be sent only to current
     * subscribers. Not caching can be used for sending instructions.
     * <br></br>
     * Topic may not exceed 255 characters. Topic along with the content may not exceed 1496 bytes.
     * @param content integer content
     * @param cache   true if caching requested, else false.
     */
    public void publish(Integer content, boolean cache) {
        PublisherSender sender = new PublisherSender(topic, destination, content, cache);
        Thread t = new Thread(sender);
        t.start();
    }
}
