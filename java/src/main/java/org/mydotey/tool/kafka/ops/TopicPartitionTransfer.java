package org.mydotey.tool.kafka.ops;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.mydotey.tool.kafka.Assignments;
import org.mydotey.tool.kafka.Brokers;
import org.mydotey.tool.kafka.Clients;

import com.google.common.io.Files;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.helper.HelpScreenException;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

/**
 * @author koqizhao
 *
 * Dec 10, 2018
 */
public class TopicPartitionTransfer {

    private static final String KEY_FROM = "from";
    private static final String KEY_TO = "to";
    private static final String KEY_ACTION = "action";
    private static final String KEY_FILE = "file";
    private static final String KEY_INTER_BROKER_LIMIT = "inter-broker-limit";
    private static final String KEY_TOPIC = "topic";
    private static final String KEY_PARTITION = "partition";

    private static final String ACTION_GENERATE = "generate";
    private static final String ACTION_EXECUTE = "execute";

    private static ArgumentParser _argumentParser = ArgumentParsers.newFor(TopicPartitionTransfer.class.getSimpleName())
            .build();

    static {
        _argumentParser.addArgument("-bs", "--" + Clients.KEY_BOOTSTRAP_SERVERS).required(true);
        _argumentParser.addArgument("-zk", "--" + Clients.KEY_ZK_CONNECT).required(true);
        _argumentParser.addArgument("-f", "--" + KEY_FROM).type(Integer.class).required(true);
        _argumentParser.addArgument("-t", "--" + KEY_TO).type(Integer.class).required(true);
        _argumentParser.addArgument("-a", "--" + KEY_ACTION).choices(ACTION_GENERATE, ACTION_EXECUTE)
                .setDefault(ACTION_GENERATE);
        _argumentParser.addArgument("--" + KEY_FILE).setDefault("assignments.json");
        _argumentParser.addArgument("-ibl", "--" + KEY_INTER_BROKER_LIMIT).type(Long.class).dest(KEY_INTER_BROKER_LIMIT)
                .setDefault(Assignments.DEFAULT_REASSIGN_THROTTLE_LIMIT);
        _argumentParser.addArgument("--" + KEY_TOPIC).required(true);
        _argumentParser.addArgument(KEY_PARTITION).type(Integer.class).nargs("*").required(true);
    }

    public static void main(String[] args) throws Exception {
        Namespace ns;
        try {
            ns = _argumentParser.parseArgs(args);
        } catch (HelpScreenException e) {
            return;
        }

        Properties properties = new Properties();
        String bootstrapServers = ns.get(Clients.KEY_BOOTSTRAP_SERVERS);
        properties.put(Clients.KEY_BOOTSTRAP_SERVERS, bootstrapServers);
        String zkConnect = ns.get(Clients.KEY_ZK_CONNECT);
        properties.put(Clients.KEY_ZK_CONNECT, zkConnect);
        int from = ns.getInt(KEY_FROM);
        int to = ns.getInt(KEY_TO);
        String action = ns.get(KEY_ACTION);
        String file = ns.get(KEY_FILE);
        long interBrokerLimit = ns.get(KEY_INTER_BROKER_LIMIT);
        String topic = ns.get(KEY_TOPIC);
        Set<Integer> partitions = new HashSet<>(ns.getList(KEY_PARTITION));
        System.out.printf(
                "arguments:\n\t%s: %s\n\t%s: %s\n\t%s: %s\n\t%s: %s\n\t%s: %s\n\t%s: %s\n\t%s: %s\n\t%s: %s\n\t%s: %s\n\n",
                Clients.KEY_BOOTSTRAP_SERVERS, bootstrapServers, Clients.KEY_ZK_CONNECT, zkConnect, KEY_FROM, from,
                KEY_TO, to, KEY_ACTION, action, KEY_FILE, file, KEY_INTER_BROKER_LIMIT, interBrokerLimit, KEY_TOPIC,
                topic, KEY_PARTITION, partitions);

        try (Clients clients = new Clients(properties)) {
            Brokers brokers = new Brokers(clients);
            Assignments assignments = new Assignments(clients);
            Map<String, Map<Integer, List<Integer>>> currentAssignmentsMap = brokers.getAssignments(from, topic,
                    partitions);
            Map<String, Map<Integer, List<Integer>>> newAssignmentsMap = brokers.generateAssignmentsForTransfer(from,
                    to, topic, partitions);
            String currentAssignmentsJson = assignments.toJson(currentAssignmentsMap);
            String newAssignmentsJson = assignments.toJson(newAssignmentsMap);
            System.out.printf("\ncurrent assignments: \n%s\n\nnew assignments:\n%s\n\n", currentAssignmentsJson,
                    newAssignmentsJson);

            String fileName = file.endsWith(".json") ? file.substring(0, file.length() - ".json".length()) : file;
            String oldFileName = fileName + ".old.json";
            String newFileName = fileName + ".new.json";
            Files.write(currentAssignmentsJson, new File(oldFileName), StandardCharsets.UTF_8);
            Files.write(newAssignmentsJson, new File(newFileName), StandardCharsets.UTF_8);
            System.out.printf("saved assignments to files: \n\t%s\n\t%s\n\n", oldFileName, newFileName);

            switch (action) {
                case ACTION_EXECUTE:
                    assignments.reassign(newAssignmentsMap, interBrokerLimit,
                            Assignments.DEFAULT_REASSIGN_THROTTLE_LIMIT, Assignments.DEFAULT_REASSIGN_TIMEOUT);
                    System.out.println();
                    break;
                default:
                    break;
            }
        }
    }

}
