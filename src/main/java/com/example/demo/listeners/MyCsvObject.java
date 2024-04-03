package com.example.demo.listeners;

import java.util.HashMap;
import java.util.Map;

public class MyCsvObject {
    private Map<String, String> data;

    public MyCsvObject() {
        data = new HashMap<>();
    }

    public void put(String key, String value) {
        data.put(key, value);
    }

    public String get(String key) {
        return data.get(key);
    }

    // Other methods as needed...
}

