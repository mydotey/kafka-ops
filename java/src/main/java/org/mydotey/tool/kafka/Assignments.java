package org.mydotey.tool.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;
import org.mydotey.scala.converter.ScalaConverters;

import kafka.admin.ReassignPartitionsCommand;
import kafka.admin.ReassignPartitionsCommand.Throttle;
import kafka.admin.ReassignmentStatus;
import kafka.utils.Json;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * @author koqizhao
 *
 * Dec 9, 2018
 */
public class Assignments {

    public static final long DEFAULT_REASSIGN_THROTTLE_LIMIT = -1;
    public static final long DEFAULT_REASSIGN_TIMEOUT = 10 * 1000;

    public enum Status {
        Completed, Failed, InProgress;

        public static Status valueOf(ReassignmentStatus kaStatus) {
            switch (kaStatus.status()) {
                case -1:
                    return Status.Failed;
                case 0:
                    return Status.InProgress;
                case 1:
                    return Status.Completed;
                default:
                    throw new IllegalArgumentException("unknown status: " + kaStatus.status());
            }
        }
    }

    private Clients _clients;

    public Assignments(Clients clients) {
        _clients = clients;
    }

    public Map<String, Map<Integer, List<Integer>>> getOfTopics(Set<String> topics) {
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

    public void reassign(Map<String, Map<Integer, List<Integer>>> assignments) {
        reassign(assignments, DEFAULT_REASSIGN_THROTTLE_LIMIT, DEFAULT_REASSIGN_THROTTLE_LIMIT,
                DEFAULT_REASSIGN_TIMEOUT);
    }

    public void reassign(Map<String, Map<Integer, List<Integer>>> assignments, long interBrokerLimit,
            long replicaAlterLogDirsLimit, long timeout) {
        String jsonString = toJson(assignments);
        Throttle throttle = new Throttle(interBrokerLimit, replicaAlterLogDirsLimit,
                ScalaConverters.to(() -> scala.runtime.BoxedUnit.UNIT));
        ReassignPartitionsCommand.executeAssignment(_clients.getKafkaZkClient(),
                Option.apply(_clients.getAdminClient()), jsonString, throttle, timeout);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Map<String, Map<Integer, Status>> verifyAssignment(Map<String, Map<Integer, List<Integer>>> assignments) {
        Map<TopicPartition, List<Integer>> assignments2 = new HashMap<>();
        assignments.forEach((t, a) -> {
            a.forEach((p, bl) -> {
                assignments2.put(new TopicPartition(t, p), bl);
            });
        });

        scala.collection.mutable.Map<TopicPartition, Seq<Object>> partitionsToBeReassigned = new scala.collection.mutable.HashMap<>();
        assignments2.forEach((tp, bl) -> {
            partitionsToBeReassigned.put(tp, (Seq) JavaConverters.asScalaBufferConverter(bl).asScala().seq());
        });
        scala.collection.Map<TopicPartition, Seq<Object>> partitionsBeingReassigned = _clients.getKafkaZkClient()
                .getPartitionReassignment();
        Map<String, Map<Integer, Status>> result = new HashMap<>();
        assignments2.keySet().forEach(tp -> {
            ReassignmentStatus status = ReassignPartitionsCommand.checkIfPartitionReassignmentSucceeded(
                    _clients.getKafkaZkClient(), tp, partitionsToBeReassigned, partitionsBeingReassigned);
            Map<Integer, Status> topicStatus = result.computeIfAbsent(tp.topic(), k -> new HashMap<>());
            topicStatus.put(tp.partition(), Status.valueOf(status));
        });

        return result;
    }

    public String toJson(Map<String, Map<Integer, List<Integer>>> assignments) {
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("version", 1);
        ArrayList<Map<String, Object>> partitions = new ArrayList<>();
        jsonMap.put("partitions", partitions);
        assignments.forEach((t, a) -> {
            a.forEach((p, bl) -> {
                Map<String, Object> partition = new HashMap<>();
                partitions.add(partition);
                partition.put("topic", t);
                partition.put("partition", p);
                partition.put("replicas", bl);
            });
        });
        return Json.encodeAsString(jsonMap);
    }

}
