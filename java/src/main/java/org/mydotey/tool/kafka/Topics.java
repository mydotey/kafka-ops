package org.mydotey.tool.kafka;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicListing;

import kafka.common.KafkaException;

/**
 * @author koqizhao
 *
 * Dec 7, 2018
 */
public class Topics {

    public static final String CONSUMER_OFFSETS = "__consumer_offsets";

    private Clients _clients;

    public Topics(Clients clients) {
        _clients = clients;
    }

    public Set<String> getAll() {
        return getAll(tl -> true);
    }

    public Set<String> getAllWithoutInternal() {
        return getAll(tl -> !tl.isInternal());
    }

    public Set<String> getAllInternal() {
        return getAll(tl -> tl.isInternal());
    }

    private Set<String> getAll(Predicate<TopicListing> filter) {
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(true);
        ListTopicsResult result = _clients.getAdminClient().listTopics(options);
        try {
            return result.listings().get().stream().filter(filter).map(tl -> tl.name()).collect(Collectors.toSet());
        } catch (InterruptedException | ExecutionException e) {
            throw new KafkaException(e);
        }
    }

}
