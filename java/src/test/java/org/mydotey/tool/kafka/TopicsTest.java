package org.mydotey.tool.kafka;

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

/**
 * @author koqizhao
 *
 * Dec 7, 2018
 */
public class TopicsTest extends KafkaTest {

    @Test
    public void getTopics() throws Exception {
        Topics topics = new Topics(getClients());
        Set<String> all = topics.getAll();
        System.out.println(all);
        Assert.assertEquals(ALL_TOPICS, all);

        all = topics.getAllWithoutInternal();
        System.out.println(all);
        Assert.assertEquals(ImmutableSet.of(TOPIC, TOPIC_2), all);

        all = topics.getAllInternal();
        System.out.println(all);
        Assert.assertEquals(ImmutableSet.of(Topics.CONSUMER_OFFSETS), all);
    }

}
