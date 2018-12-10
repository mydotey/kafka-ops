package org.mydotey.tool.kafka;

import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.utils.Time;

import kafka.server.KafkaConfig;
import kafka.zk.KafkaZkClient;
import kafka.zookeeper.ZooKeeperClient;

/**
 * @author koqizhao
 *
 * Dec 7, 2018
 */
public class Clients implements AutoCloseable {

    public static final String KEY_BOOTSTRAP_SERVERS = AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;
    public static final String KEY_ZK_CONNECT = KafkaConfig.ZkConnectProp();
    public static final String KEY_CLIENT_ID = AdminClientConfig.CLIENT_ID_CONFIG;

    public static final String DEFAULT_CLIENT_ID = "kafka-ops";

    private AdminClient _adminClient;
    private ZooKeeperClient _zkClient;
    private KafkaZkClient _kafkaZkClient;

    public Clients(Properties properties) {
        String clientId = properties.getProperty(KEY_CLIENT_ID);
        if (clientId == null || clientId.isEmpty())
            properties.setProperty(KEY_CLIENT_ID, DEFAULT_CLIENT_ID);

        _adminClient = AdminClient.create(properties);
        String zkConnect = properties.getProperty(KEY_ZK_CONNECT);
        _zkClient = new ZooKeeperClient(zkConnect, 30 * 1000, 30 * 1000, Integer.MAX_VALUE, Time.SYSTEM,
                DEFAULT_CLIENT_ID, "clients");
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
