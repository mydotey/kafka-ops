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
    public void listTopics() throws Exception {
        try (Clients clients = newClients();) {
            Topics topics = new Topics(clients);
            Set<String> listed = topics.list();
            System.out.println(listed);
            Assert.assertNotNull(listed);
            Assert.assertTrue(!listed.isEmpty());
            Assert.assertTrue(listed.contains(Topics.INTERNAL_CONSUMER_OFFSETS));

            listed = topics.list(false);
            System.out.println(listed);
            Assert.assertTrue(!listed.contains(Topics.INTERNAL_CONSUMER_OFFSETS));
        }
    }

    @Test
    public void listTopicsOfBroker() throws Exception {
        try (Clients clients = newClients()) {
            Topics topics = new Topics(clients);
            int brokerId = 2;
            Set<String> replicas = topics.list(brokerId);
            System.out.println(replicas);
        }
    }

}
