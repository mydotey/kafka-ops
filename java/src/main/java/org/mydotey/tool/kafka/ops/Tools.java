package org.mydotey.tool.kafka.ops;

import java.util.HashMap;

public class Tools {

    private static HashMap<Class<?>, String> _tools = new HashMap<>();

    static {
        _tools.put(BrokerDataTransfer.class, "transfer data from 1 broker to another");
    }

    public static void main(String[] args) {
        System.out.println("kafka ops tool list\n");
        _tools.forEach((t, d) -> {
            System.out.printf("class: %s\ndetails:\n%s\n\n", t.getName(), d);
        });
    }

}
