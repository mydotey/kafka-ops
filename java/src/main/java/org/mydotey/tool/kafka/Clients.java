package org.mydotey.tool.kafka;

import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.utils.Time;

import kafka.zk.KafkaZkClient;
import kafka.zookeeper.ZooKeeperClient;

/**
 * @author koqizhao
 *
 * Dec 7, 2018
 */
public class Clients implements AutoCloseable {

    public static final String KEY_BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String KEY_ZK_CONNECT = "zookeeper.connect";

    private AdminClient _adminClient;
    private ZooKeeperClient _zkClient;
    private KafkaZkClient _kafkaZkClient;

    public Clients(Properties properties) {
        _adminClient = AdminClient.create(properties);
        String zkConnect = properties.getProperty(KEY_ZK_CONNECT);
        _zkClient = new ZooKeeperClient(zkConnect, 30 * 1000, 30 * 1000, Integer.MAX_VALUE, Time.SYSTEM, "kafka-ops",
                "clients");
        _kafkaZkClient = new KafkaZkClient(_zkClient, false, Time.SYSTEM);
    }

    public AdminClient getAdminClient() {
        return _adminClient;
    }

    public ZooKeeperClient getZkClient() {
        return _zkClient;
    }

    public KafkaZkClient getKafkaZkClient() {
        return _kafkaZkClient;
    }

    @Override
    public void close() throws Exception {
        _adminClient.close();
        _kafkaZkClient.close();
        _zkClient.close();
    }

}
