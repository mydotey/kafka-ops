package org.mydotey.tool.kafka.ops;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.mydotey.tool.kafka.ops.Brokers;
import org.mydotey.tool.kafka.ops.Topics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * @author koqizhao
 *
 * Dec 7, 2018
 */
public class BrokersTest extends KafkaTest {

    @Test
    public void getAll() throws Exception {
        Brokers brokers = new Brokers(getClients());
        Set<Integer> expected = ALL_BROKERS;
        Set<Integer> actual = brokers.getAll();
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void getTopics() throws Exception {
        Brokers brokers = new Brokers(getClients());
        Set<String> expected = ImmutableSet.of(Topics.CONSUMER_OFFSETS, TOPIC_2);
        Set<String> actual = brokers.getTopics(BROKER_3);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void getTopicPartitons() {
        Brokers brokers = new Brokers(getClients());
        Map<String, Set<Integer>> expected = ImmutableMap.of(TOPIC_2, ImmutableSet.of(1, 2, 3));
        Map<String, Set<Integer>> actual = brokers.getTopicPartitons(BROKER_3, ImmutableSet.of(TOPIC_2));
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void getAssignments() {
        Brokers brokers = new Brokers(getClients());
        Map<String, Map<Integer, List<Integer>>> expected = ImmutableMap.of(TOPIC,
                ImmutableMap.of(1, ImmutableList.of(BROKER_2), 3, ImmutableList.of(BROKER_2), 5,
                        ImmutableList.of(BROKER_2), 7, ImmutableList.of(BROKER_2), 9, ImmutableList.of(BROKER_2)));
        Map<String, Map<Integer, List<Integer>>> actual = brokers.getAssignments(BROKER_2, ImmutableSet.of(TOPIC));
        Assert.assertEquals(expected, actual);

        expected = ImmutableMap.of(TOPIC_2, ImmutableMap.of(1, ImmutableList.of(BROKER_2, BROKER_3), 2,
                ImmutableList.of(BROKER_3, BROKER), 3, ImmutableList.of(BROKER, BROKER_3)));
        actual = brokers.getAssignments(BROKER_3, ImmutableSet.of(TOPIC_2));
        Assert.assertEquals(expected, actual);

        expected = ImmutableMap.of(TOPIC,
                ImmutableMap.of(1, ImmutableList.of(BROKER_2), 3, ImmutableList.of(BROKER_2)));
        actual = brokers.getAssignments(BROKER_2, TOPIC, ImmutableSet.of(1, 3));
        Assert.assertEquals(expected, actual);

        expected = ImmutableMap.of(TOPIC_2,
                ImmutableMap.of(1, ImmutableList.of(BROKER_2, BROKER_3), 2, ImmutableList.of(BROKER_3, BROKER)));
        actual = brokers.getAssignments(BROKER_3, TOPIC_2, ImmutableSet.of(1, 2));
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void generateAssignmentsForTransfer() {
        Brokers brokers = new Brokers(getClients());
        Map<String, Map<Integer, List<Integer>>> expected = ImmutableMap.of(TOPIC,
                ImmutableMap.of(1, ImmutableList.of(BROKER_3), 3, ImmutableList.of(BROKER_3), 5,
                        ImmutableList.of(BROKER_3), 7, ImmutableList.of(BROKER_3), 9, ImmutableList.of(BROKER_3)));
        Map<String, Map<Integer, List<Integer>>> actual = brokers.generateAssignmentsForTransfer(BROKER_2,
                Arrays.asList(BROKER_3), ImmutableSet.of(TOPIC));
        Assert.assertEquals(expected, actual);
    }

}
