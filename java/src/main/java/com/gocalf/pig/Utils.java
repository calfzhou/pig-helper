package com.gocalf.pig;

import java.util.Map;

public class Utils {

    public static <K, V> V mapGet(Map<K, V> map, K key, V defaultValue) {
        V value = map.get(key);
        return (value == null) ? defaultValue : value;
    }
}
