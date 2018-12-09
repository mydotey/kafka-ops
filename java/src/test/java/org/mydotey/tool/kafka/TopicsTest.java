package org.mydotey.tool.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author koqizhao
 *
 * Dec 7, 2018
 */
public class TopicsTest {

    protected Clients newClients() throws IOException {
        Properties properties = new Properties();
        try (InputStream is = TopicsTest.class.getResourceAsStream("/local-kafka.properties")) {
            properties.load(is);
        }

        return new Clients(properties);
    }

    @Test
    public void getTopics() throws Exception {
        try (Clients clients = newClients();) {
            Topics topics = new Topics(clients);
            Set<String> all = topics.getAll();
            System.out.println(all);
            Assert.assertNotNull(all);
            Assert.assertTrue(!all.isEmpty());
            Assert.assertTrue(all.contains(Topics.INTERNAL_CONSUMER_OFFSETS));

            all = topics.getAllWithoutInternal();
            System.out.println(all);
            Assert.assertTrue(!all.contains(Topics.INTERNAL_CONSUMER_OFFSETS));
        }
    }

    @Test
    public void getOfBroker() throws Exception {
        try (Clients clients = newClients()) {
            Topics topics = new Topics(clients);
            int brokerId = 2;
            Set<String> replicas = topics.getOfBroker(brokerId);
            System.out.println(replicas);
        }
    }

}
