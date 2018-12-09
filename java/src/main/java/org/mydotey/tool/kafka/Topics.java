package org.mydotey.tool.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;

import kafka.common.KafkaException;
import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * @author koqizhao
 *
 * Dec 7, 2018
 */
public class Topics {

    public static final String INTERNAL_CONSUMER_OFFSETS = "__consumer_offsets";

    private Clients _clients;

    public Topics(Clients clients) {
        _clients = clients;
    }

    public Set<String> getAll() {
        return getAll(true);
    }

    public Set<String> getAllWithoutInternal() {
        return getAll(false);
    }

    private Set<String> getAll(boolean includeInternal) {
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(includeInternal);
        ListTopicsResult result = _clients.getAdminClient().listTopics(options);
        try {
            return result.names().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new KafkaException(e);
        }
    }

    public Set<String> getAllInternal() {
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(true);
        ListTopicsResult result = _clients.getAdminClient().listTopics(options);
        try {
            return result.listings().get().stream().filter(t -> t.isInternal()).map(tl -> tl.name())
                    .collect(Collectors.toSet());
        } catch (InterruptedException | ExecutionException e) {
            throw new KafkaException(e);
        }
    }

    public Set<String> getOfBroker(int brokerId) {
        Map<String, Map<Integer, List<Integer>>> assignmentMap = getAllAssignments();
        Set<String> topics = new HashSet<>();
        assignmentMap.forEach((t, a) -> {
            a.values().forEach(l -> {
                if (l.contains(brokerId))
                    topics.add(t);
            });
        });
        return topics;
    }

    public Map<String, Map<Integer, List<Integer>>> getAllAssignments() {
        Set<String> topics = getAll();
        scala.collection.mutable.Set<String> scTopics = JavaConverters.asScalaSetConverter(topics).asScala();
        scala.collection.immutable.Map<String, scala.collection.immutable.Map<Object, Seq<Object>>> scAssignmentsMap = _clients
                .getKafkaZkClient().getPartitionAssignmentForTopics(scTopics.toSet());
        Map<String, Map<Integer, List<Integer>>> assignmentMap = new HashMap<>();
        JavaConverters.mapAsJavaMapConverter(scAssignmentsMap).asJava().forEach((t, m) -> {
            Map<Integer, List<Integer>> topicAssignments = assignmentMap.computeIfAbsent(t, k -> new HashMap<>());
            JavaConverters.mapAsJavaMapConverter(m).asJava().forEach((p, a) -> {
                List<Integer> replicas = topicAssignments.computeIfAbsent((Integer) p, k -> new ArrayList<>());
                JavaConverters.seqAsJavaListConverter(a).asJava().forEach(r -> replicas.add((Integer) r));
            });
        });
        return assignmentMap;
    }

}
