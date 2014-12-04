package com.kylinolap.common.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by qianzhou on 12/3/14.
 */
public class CaseInsensitiveStringMap<T> implements Map<String, T> {

    private Map<String, T> innerMap;

    public CaseInsensitiveStringMap(){
        this(new HashMap<String, T>());
    }
    public CaseInsensitiveStringMap(Map<String, T> innerMap){
        this.innerMap = innerMap;
    }


    @Override
    public int size() {
        return innerMap.size();
    }

    @Override
    public boolean isEmpty() {
        return innerMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return key != null?innerMap.containsKey(key.toString().toUpperCase()):false;
    }

    @Override
    public boolean containsValue(Object value) {
        return value != null?innerMap.containsValue(value):false;
    }

    @Override
    public T get(Object key) {
        return key != null?innerMap.get(key.toString().toUpperCase()):null;
    }

    @Override
    public T put(String key, T value) {
        return key != null?innerMap.put(key.toString().toUpperCase(), value):null;
    }

    @Override
    public T remove(Object key) {
        return key != null?innerMap.remove(key.toString().toUpperCase()):null;
    }

    @Override
    public void putAll(Map<? extends String, ? extends T> m) {
        innerMap.putAll(m);
    }

    @Override
    public void clear() {
        innerMap.clear();
    }

    @Override
    public Set<String> keySet() {
        return innerMap.keySet();
    }

    @Override
    public Collection<T> values() {
        return innerMap.values();
    }

    @Override
    public Set<Entry<String, T>> entrySet() {
        return innerMap.entrySet();
    }

    @Override
    public String toString() {
        return this.innerMap.toString();
    }
}
