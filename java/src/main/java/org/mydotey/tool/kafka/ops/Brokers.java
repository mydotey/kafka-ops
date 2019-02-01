package org.mydotey.tool.kafka.ops;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;

import kafka.cluster.Broker;
import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * @author koqizhao
 *
 * Dec 9, 2018
 */
public class Brokers {

    private Clients _clients;

    public Brokers(Clients clients) {
        _clients = clients;
    }

    public Set<Integer> getAll() {
        Seq<Broker> brokers = _clients.getKafkaZkClient().getAllBrokersInCluster();
        List<Broker> jBrokers = JavaConverters.seqAsJavaListConverter(brokers).asJava();
        return jBrokers.stream().map(b -> b.id()).collect(Collectors.toSet());
    }

    public Set<String> getTopics(int broker) {
        Set<String> topics = new Topics(_clients).getAll();
        return getAssignments(broker, topics).keySet();
    }

    public Map<String, Set<Integer>> getTopicPartitons(int broker, Set<String> topics) {
        Map<String, Set<Integer>> result = new HashMap<>();
        getAssignments(broker, topics).forEach((t, a) -> result.put(t, a.keySet()));
        return result;
    }

    public Map<String, Map<Integer, List<Integer>>> getAssignments(int broker, Set<String> topics) {
        Map<String, Map<Integer, List<Integer>>> assignments = new Assignments(_clients).getOfTopics(topics);
        Map<String, Map<Integer, List<Integer>>> results = new HashMap<>();
        assignments.forEach((t, a) -> {
            a.forEach((p, bl) -> {
                if (bl.contains(broker)) {
                    Map<Integer, List<Integer>> topicAssignments = results.computeIfAbsent(t, k -> new HashMap<>());
                    topicAssignments.put(p, bl);
                }
            });
        });
        return results;
    }

    public Map<String, Map<Integer, List<Integer>>> getAssignments(int broker, String topic, Set<Integer> partitions) {
        Map<String, Map<Integer, List<Integer>>> assignments = new Assignments(_clients)
                .getOfTopics(ImmutableSet.of(topic));
        Map<String, Map<Integer, List<Integer>>> results = new HashMap<>();
        assignments.forEach((t, a) -> {
            a.forEach((p, bl) -> {
                if (bl.contains(broker) && partitions.contains(p)) {
                    Map<Integer, List<Integer>> topicAssignments = results.computeIfAbsent(t, k -> new HashMap<>());
                    topicAssignments.put(p, bl);
                }
            });
        });
        return results;
    }

    public Map<String, Map<Integer, List<Integer>>> generateAssignmentsForTransfer(int from, List<Integer> tos,
            Set<String> topics) {
        Map<String, Map<Integer, List<Integer>>> assignments = getAssignments(from, topics);
        changeAssignmentsForTransfer(from, tos, assignments);
        return assignments;
    }

    public Map<String, Map<Integer, List<Integer>>> generateAssignmentsForTransfer(int from, List<Integer> tos,
            String topic, Set<Integer> partitions) {
        Map<String, Map<Integer, List<Integer>>> assignments = getAssignments(from, topic, partitions);
        changeAssignmentsForTransfer(from, tos, assignments);
        return assignments;
    }

    protected void changeAssignmentsForTransfer(int from, List<Integer> tos,
            Map<String, Map<Integer, List<Integer>>> assignments) {
        assignments.forEach((t, a) -> {
            a.forEach((p, bl) -> {
                AtomicBoolean reassignSuccess = new AtomicBoolean();
                tos.forEach(to -> {
                    if (bl.contains(to) || reassignSuccess.get())
                        return;

                    bl.replaceAll(b -> b == from ? to : b);
                    reassignSuccess.set(true);
                });

                if (reassignSuccess.get())
                    return;

                String errorMessage = String
                        .format("tos (brokers: %s) has been in the assignment list for partition: {%s, %s}", tos, t, p);
                throw new IllegalArgumentException(errorMessage);
            });
        });
    }

}
