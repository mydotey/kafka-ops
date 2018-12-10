package org.mydotey.tool.kafka.ops;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;

import net.sourceforge.argparse4j.helper.HelpScreenException;

public class Tools {

    private static HashMap<Class<?>, String> _tools = new HashMap<>();

    static {
        _tools.put(BrokerDataTransfer.class, "transfer data from 1 broker to another");
        _tools.put(ReassignmentStatusPrinter.class, "show reassignment status");
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("kafka ops tool list\n");
            _tools.forEach((t, d) -> {
                System.out.printf("class: %s\ndetails:\n%s\n\n", t.getName(), d);
            });

            return;
        }

        Class<?> clazz = Class.forName(args[0]);
        if (!_tools.containsKey(clazz)) {
            System.err.println("unknow tool: " + clazz);
            return;
        }

        Method mainMethod = clazz.getMethod("main", String[].class);
        try {
            mainMethod.invoke(null, new Object[] { Arrays.copyOfRange(args, 1, args.length) });
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof HelpScreenException)
                return;

            cause.printStackTrace();
        }
    }

}
