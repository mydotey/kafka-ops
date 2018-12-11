package org.mydotey.tool.kafka.ops;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.mydotey.tool.kafka.ops.Assignments;
import org.mydotey.tool.kafka.ops.Brokers;
import org.mydotey.tool.kafka.ops.Topics;
import org.mydotey.tool.kafka.ops.Assignments.Status;
import org.mydotey.util.FileUtil;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * @author koqizhao
 *
 * Dec 7, 2018
 */
public class AssignmentsTest extends KafkaTest {

    @Test
    public void getAssignments() throws Exception {
        Assignments assignments = new Assignments(getClients());
        Topics topics = new Topics(getClients());

        Map<String, Map<Integer, List<Integer>>> expected = ALL_ASSIGNMENTS;
        Map<String, Map<Integer, List<Integer>>> actual = assignments.getOfTopics(topics.getAll());
        System.out.println(actual);
        Assert.assertEquals(expected, actual);

        expected = ImmutableMap.of(Topics.CONSUMER_OFFSETS, CONSUMER_OFFSETSETS_ASSIGNMENTS);
        actual = assignments.getOfTopics(ImmutableSet.of(Topics.CONSUMER_OFFSETS));
        System.out.println(actual);
        Assert.assertEquals(expected, actual);

        expected = ImmutableMap.of(TOPIC, TOPIC_ASSIGNMENTS);
        actual = assignments.getOfTopics(ImmutableSet.of(TOPIC));
        System.out.println(actual);
        Assert.assertEquals(expected, actual);

        expected = ImmutableMap.of(TOPIC_2, TOPIC_2_ASSIGNMENTS);
        actual = assignments.getOfTopics(ImmutableSet.of(TOPIC_2));
        System.out.println(actual);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void toJson() {
        Assignments assignments = new Assignments(getClients());
        String jsonString = assignments.toJson(ALL_ASSIGNMENTS);
        System.out.println(jsonString);
    }

    @Test
    public void fromJson() throws IOException, URISyntaxException {
        Assignments assignments = new Assignments(getClients());
        URI testFile = new URI(AssignmentsTest.class.getResource("/assignments.json").toString());
        String jsonString = FileUtil.readFileContent(Paths.get(testFile));
        Map<String, Map<Integer, List<Integer>>> assignmentsMap = assignments.fromJson(jsonString);
        System.out.println(assignmentsMap);
    }

    @Test
    public void reassignTest() throws InterruptedException {
        Brokers brokers = new Brokers(getClients());
        Assignments assignments = new Assignments(getClients());
        try {
            Map<String, Map<Integer, List<Integer>>> assignmentsMap = brokers.generateAssignmentsForTransfer(BROKER_2,
                    BROKER_3, ImmutableSet.of(TOPIC));
            assignments.reassign(assignmentsMap);
            ensureReassignmentComplete(assignments, assignmentsMap);
        } finally {
            Map<String, Map<Integer, List<Integer>>> assignmentsMap = brokers.generateAssignmentsForTransfer(BROKER_3,
                    BROKER_2, ImmutableSet.of(TOPIC));
            assignments.reassign(assignmentsMap);
            ensureReassignmentComplete(assignments, assignmentsMap);
        }
    }

    protected void ensureReassignmentComplete(Assignments assignments,
            Map<String, Map<Integer, List<Integer>>> assignmentsMap) throws InterruptedException {
        System.out.println("\nassignments:");
        System.out.println(assignmentsMap);
        Map<String, Map<Integer, Status>> statusMap = assignments.verifyAssignment(assignmentsMap);
        while (statusMap.entrySet().stream().anyMatch(e -> {
            return e.getValue().entrySet().stream().anyMatch(e2 -> e2.getValue() == Status.InProgress);
        })) {
            Thread.sleep(10);
            statusMap = assignments.verifyAssignment(assignmentsMap);
        }

        System.out.println("\nresult:");
        System.out.println(statusMap);

        if (statusMap.entrySet().stream().anyMatch(e -> {
            return e.getValue().entrySet().stream().anyMatch(e2 -> e2.getValue() == Status.Failed);
        }))
            Assert.fail("reassignment failed");
    }

}
