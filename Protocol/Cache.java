package Protocol;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Dominik Guzowski, 19334866
 */

public class Cache<T> {
    private int maxCacheLength = 16;
    private ArrayList<T> content;
    private String topicName;
    private HashMap<String, Cache<T>> subTopics;
    public Cache() {
        topicName = "*";
        content = null;
        subTopics = new HashMap<String, Cache<T>>();
    }

    public boolean hasTopic(String topic) {
        return subTopics.keySet().contains(topic);
    }

    private Cache(String name) {
        topicName = name;
        content = new ArrayList<T>();
        subTopics = new HashMap<String, Cache<T>>();
    }

    public void addContent(String topicPath, T newContent) {
        if(topicPath == null) return;
        addContent(topicPath.split("/"), newContent);
    }

    public void addContent(String[] topicPath, T newContent) {
        if(topicPath == null) return;
        if(topicPath.length == 0) return;
        addContent(new ArrayList<String>(Arrays.asList(topicPath)), newContent, this);
    }

    private boolean addContent(ArrayList<String> topicPath, T newContent, Cache<T> cache) {
        if(topicPath.size() > 0) {
            if(topicPath.get(0).equals("*")) return false;
            else {
                if(!cache.subTopics.keySet().contains(topicPath.get(0))) {
                    cache.subTopics.put(topicPath.get(0), new Cache<T>(topicPath.get(0)));
                }
                String topic = topicPath.remove(0);
                return cache.subTopics.get(topic).addContent(topicPath, newContent, cache.subTopics.get(topic));
            }
        } else {
            if(newContent != null) content.add(newContent);

            if(content.size() > maxCacheLength) content.remove(0);
            return true;
        }
    }

    public void shallowRemove(String topic, T content) {
        try {
            subTopics.get(topic).content.remove(content);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    public HashMap<String, ArrayList<T>> get(String path) {
        if(path == null) return null;
        path = path.replaceAll("/+", "/");
        Pattern p = Pattern.compile("\\*");
        Matcher m = p.matcher(path);
        int count = 0;
        while(m.find()) {
            count++;
        }
        if(count > 1) {
            return new HashMap<String, ArrayList<T>>();
        }
        if(count == 1) {
            if(path.lastIndexOf("/*") == path.length() - 2) {
                return getAll(path);
            } else return new HashMap<String, ArrayList<T>>();
        }
        ArrayList<T> list = get(path.split("/"));
        HashMap<String, ArrayList<T>> res = new HashMap<String, ArrayList<T>>();
        res.put(path, list);
        return res;
    }

    public ArrayList<T> get(String[] path) {
        return get(new ArrayList<String>(Arrays.asList(path)), this);
    }

    private ArrayList<T> get(ArrayList<String> path, Cache<T> cache) {
        if(cache == null) return null;
        if(path.size() == 0) return new ArrayList<T>(cache.content);
        else {
            String topic = path.remove(0);
            return get(path, cache.subTopics.get(topic));
        }
    }
    @Override
    public String toString() {
        return "Topic: " + topicName + ", Content: " + (content == null? 0 : content.size()) + ", Direct SubTopics: " + (subTopics == null ? 0 : subTopics.size() );
    }

    @Override
    public int hashCode() {
        return topicName.hashCode();
    }

    public ArrayList<String> getTopics() {
        return new ArrayList<String>(subTopics.keySet());
    }
    public HashMap<String, ArrayList<T>> getAll(String path) {
        HashMap<String, ArrayList<T>> result = new HashMap<String, ArrayList<T>>();
        getAll(new ArrayList<String>(Arrays.asList(path.split("/"))), new ArrayList<String>(), null, result);
        ArrayList<String> keys = new ArrayList<String>();
        for(String key : result.keySet()) {
            if(result.get(key).size() == 0) {
                keys.add(key);
            }
        }
        for(String key : keys) {
            result.remove(key);
        }
        return result;
    }

    private void getAll(ArrayList<String> path, ArrayList<String> currentPath, Cache<T> cache, HashMap<String, ArrayList<T>> result) {
        if(cache == null) {
            Cache<T> current = this;
            for(String p : path) {
                if(!p.equals("*")) {
                    current = current.subTopics.get(p);
                    currentPath.add(p);
                }
                else break;
            }
            //if(current != null) result.put(stringifyPath(currentPath), current.content);
            cache = current;
        }
        else 
        {
            currentPath.add(cache.topicName);
            result.put(stringifyPath(currentPath), cache.content);
        }
        if(cache != null)
        for(String key : cache.subTopics.keySet()) {
            getAll(path, currentPath, cache.subTopics.get(key), result);
        }
        if(currentPath.size() - 1 >= 0) currentPath.remove(currentPath.size() - 1);
    }

    private String stringifyPath(ArrayList<String> pathList) {
        String path = "";
        for(String p : pathList) {
            path += p + "/";
        }
        path = path.substring(0, path.length() - 1);
        return path;
    }

    public boolean setMaxCacheLength(int len) {
        if(len > 0) {
            maxCacheLength = len;
            return true;
        } else return false;
    }

    public int getMaxLength() {
        return maxCacheLength;
    }
}