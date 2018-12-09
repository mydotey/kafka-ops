package org.mydotey.tool.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.After;
import org.junit.Before;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * @author koqizhao
 *
 * Dec 9, 2018
 */
public class KafkaTest {

    public static final int BROKER = 0;
    public static final int BROKER_2 = 1;
    public static final int BROKER_3 = 2;
    public static final Set<Integer> ALL_BROKERS = ImmutableSet.of(BROKER, BROKER_2, BROKER_3);

    public static final String TOPIC = "test";
    public static final String TOPIC_2 = "test2";
    public static final Set<String> ALL_TOPICS = ImmutableSet.of(Topics.CONSUMER_OFFSETS, TOPIC, TOPIC_2);

    public static final Map<Integer, List<Integer>> CONSUMER_OFFSETSETS_ASSIGNMENTS = ImmutableMap
            .<Integer, List<Integer>> builder().put(0, ImmutableList.of(1)).put(1, ImmutableList.of(2))
            .put(2, ImmutableList.of(0)).put(3, ImmutableList.of(1)).put(4, ImmutableList.of(2))
            .put(5, ImmutableList.of(0)).put(6, ImmutableList.of(1)).put(7, ImmutableList.of(2))
            .put(8, ImmutableList.of(0)).put(9, ImmutableList.of(1)).put(10, ImmutableList.of(2))
            .put(11, ImmutableList.of(0)).put(12, ImmutableList.of(1)).put(13, ImmutableList.of(2))
            .put(14, ImmutableList.of(0)).put(15, ImmutableList.of(1)).put(16, ImmutableList.of(2))
            .put(17, ImmutableList.of(0)).put(18, ImmutableList.of(1)).put(19, ImmutableList.of(2))
            .put(20, ImmutableList.of(0)).put(21, ImmutableList.of(1)).put(22, ImmutableList.of(2))
            .put(23, ImmutableList.of(0)).put(24, ImmutableList.of(1)).put(25, ImmutableList.of(2))
            .put(26, ImmutableList.of(0)).put(27, ImmutableList.of(1)).put(28, ImmutableList.of(2))
            .put(29, ImmutableList.of(0)).put(30, ImmutableList.of(1)).put(31, ImmutableList.of(2))
            .put(32, ImmutableList.of(0)).put(33, ImmutableList.of(1)).put(34, ImmutableList.of(2))
            .put(35, ImmutableList.of(0)).put(36, ImmutableList.of(1)).put(37, ImmutableList.of(2))
            .put(38, ImmutableList.of(0)).put(39, ImmutableList.of(1)).put(40, ImmutableList.of(2))
            .put(41, ImmutableList.of(0)).put(42, ImmutableList.of(1)).put(43, ImmutableList.of(2))
            .put(44, ImmutableList.of(0)).put(45, ImmutableList.of(1)).put(46, ImmutableList.of(2))
            .put(47, ImmutableList.of(0)).put(48, ImmutableList.of(1)).put(49, ImmutableList.of(2)).build();
    public static final Map<Integer, List<Integer>> TOPIC_ASSIGNMENTS = ImmutableMap.<Integer, List<Integer>> builder()
            .put(0, ImmutableList.of(0)).put(1, ImmutableList.of(1)).put(2, ImmutableList.of(0))
            .put(3, ImmutableList.of(1)).put(4, ImmutableList.of(0)).put(5, ImmutableList.of(1))
            .put(6, ImmutableList.of(0)).put(7, ImmutableList.of(1)).put(8, ImmutableList.of(0))
            .put(9, ImmutableList.of(1)).build();
    public static final Map<Integer, List<Integer>> TOPIC_2_ASSIGNMENTS = ImmutableMap.of(0, ImmutableList.of(0, 1), 1,
            ImmutableList.of(1, 2), 2, ImmutableList.of(2, 0), 3, ImmutableList.of(0, 2), 4, ImmutableList.of(1, 0));
    public static final Map<String, Map<Integer, List<Integer>>> ALL_ASSIGNMENTS = ImmutableMap.of(
            Topics.CONSUMER_OFFSETS, CONSUMER_OFFSETSETS_ASSIGNMENTS, TOPIC, TOPIC_ASSIGNMENTS, TOPIC_2,
            TOPIC_2_ASSIGNMENTS);

    private Clients _clients;

    @Before
    public void setUp() throws IOException {
        _clients = newClients();
    }

    @After
    public void tearDown() throws Exception {
        _clients.close();
    }

    protected Clients getClients() {
        return _clients;
    }

    protected Clients newClients() throws IOException {
        Properties properties = new Properties();
        try (InputStream is = TopicsTest.class.getResourceAsStream("/local-kafka.properties")) {
            properties.load(is);
        }

        return new Clients(properties);
    }

}
