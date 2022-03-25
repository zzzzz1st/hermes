package pl.allegro.tech.hermes.consumers.supervisor.workload;

import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.api.SubscriptionName;
import pl.allegro.tech.hermes.common.config.ConfigFactory;
import pl.allegro.tech.hermes.common.metric.HermesMetrics;
import pl.allegro.tech.hermes.consumers.registry.ConsumerNodesRegistry;
import pl.allegro.tech.hermes.consumers.subscription.cache.SubscriptionsCache;
import pl.allegro.tech.hermes.domain.workload.constraints.ConsumersWorkloadConstraints;
import pl.allegro.tech.hermes.domain.workload.constraints.WorkloadConstraints;
import pl.allegro.tech.hermes.domain.workload.constraints.WorkloadConstraintsRepository;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toList;
import static pl.allegro.tech.hermes.common.config.Configs.CONSUMER_WORKLOAD_CONSUMERS_PER_SUBSCRIPTION;
import static pl.allegro.tech.hermes.common.config.Configs.CONSUMER_WORKLOAD_MAX_SUBSCRIPTIONS_PER_CONSUMER;

class BalancingJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(BalancingJob.class);

    private final ConsumerNodesRegistry consumersRegistry;
    private final ConfigFactory configFactory;
    private final SubscriptionsCache subscriptionsCache;
    private final ClusterAssignmentCache clusterAssignmentCache;
    private final ConsumerAssignmentRegistry consumerAssignmentRegistry;
    private final WorkBalancer workBalancer;
    private final HermesMetrics metrics;
    private final String kafkaCluster;
    private final WorkloadConstraintsRepository workloadConstraintsRepository;
    private final ScheduledExecutorService executorService;

    private final int intervalSeconds;

    private ScheduledFuture job;

    private final BalancingJobMetrics balancingMetrics = new BalancingJobMetrics();

    BalancingJob(ConsumerNodesRegistry consumersRegistry,
                 ConfigFactory configFactory,
                 SubscriptionsCache subscriptionsCache,
                 ClusterAssignmentCache clusterAssignmentCache,
                 ConsumerAssignmentRegistry consumerAssignmentRegistry,
                 WorkBalancer workBalancer,
                 HermesMetrics metrics,
                 int intervalSeconds,
                 String kafkaCluster,
                 WorkloadConstraintsRepository workloadConstraintsRepository) {
        this.consumersRegistry = consumersRegistry;
        this.configFactory = configFactory;
        this.subscriptionsCache = subscriptionsCache;
        this.clusterAssignmentCache = clusterAssignmentCache;
        this.consumerAssignmentRegistry = consumerAssignmentRegistry;
        this.workBalancer = workBalancer;
        this.metrics = metrics;
        this.kafkaCluster = kafkaCluster;
        this.workloadConstraintsRepository = workloadConstraintsRepository;
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("BalancingExecutor-%d").build();
        this.executorService = Executors.newSingleThreadScheduledExecutor(threadFactory);
        this.intervalSeconds = intervalSeconds;

        metrics.registerGauge(
                gaugeName(kafkaCluster, "selective.all-assignments"),
                () -> balancingMetrics.allAssignments
        );

        metrics.registerGauge(
                gaugeName(kafkaCluster, "selective.missing-resources"),
                () -> balancingMetrics.missingResources
        );
        metrics.registerGauge(
                gaugeName(kafkaCluster, ".selective.deleted-assignments"),
                () -> balancingMetrics.deletedAssignments
        );
        metrics.registerGauge(
                gaugeName(kafkaCluster, ".selective.created-assignments"),
                () -> balancingMetrics.createdAssignments
        );
    }

    private String gaugeName(String kafkaCluster, String name) {
        return "consumers-workload." + kafkaCluster + "." + name;
    }

    @Override
    public void run() {
        try {
            consumersRegistry.refresh();
            if (consumersRegistry.isLeader()) {
                try (Timer.Context ctx = metrics.consumersWorkloadRebalanceDurationTimer(kafkaCluster).time()) {
                    logger.info("Initializing workload balance.");
                    clusterAssignmentCache.refresh();

                    SubscriptionAssignmentView initialState = clusterAssignmentCache.createSnapshot();

                    ConsumersWorkloadConstraints constraints = workloadConstraintsRepository.getConsumersWorkloadConstraints();
                    WorkloadConstraints workloadConstraints = new WorkloadConstraints(
                            constraints.getSubscriptionConstraints(),
                            constraints.getTopicConstraints(),
                            configFactory.getIntProperty(CONSUMER_WORKLOAD_CONSUMERS_PER_SUBSCRIPTION),
                            configFactory.getIntProperty(CONSUMER_WORKLOAD_MAX_SUBSCRIPTIONS_PER_CONSUMER),
                            consumersRegistry.listConsumerNodes().size());

                    List<SubscriptionName> activeSubscriptions = subscriptionsCache.listActiveSubscriptionNames();
                    List<String> activeConsumerNodes = consumersRegistry.listConsumerNodes();
                    List<SubscriptionName> removedSubscriptions = findRemovedSubscriptions(initialState, activeSubscriptions);
                    List<String> inactiveConsumers = findInactiveConsumers(initialState, activeConsumerNodes);
                    List<SubscriptionName> newSubscriptions = findNewSubscriptions(initialState, activeSubscriptions);
                    List<String> newConsumers = findNewConsumers(initialState, activeConsumerNodes);

                    SubscriptionAssignmentView balancedState = workBalancer.balance(
                            new RecentChanges(removedSubscriptions, inactiveConsumers, newSubscriptions, newConsumers),
                            initialState,
                            workloadConstraints);

                    log(activeSubscriptions, activeConsumerNodes, initialState, balancedState);

                    WorkBalancingResult work = new WorkBalancingResult.Builder(balancedState)
                            .withSubscriptionsStats(activeSubscriptions.size(), removedSubscriptions.size(), newSubscriptions.size())
                            .withConsumersStats(activeConsumerNodes.size(), inactiveConsumers.size(), newConsumers.size())
                            .withMissingResources(countMissingResources(activeSubscriptions, balancedState, workloadConstraints))
                            .build();

                    if (consumersRegistry.isLeader()) {
                        logger.info("Applying workload balance changes {}", work.toString());
                        WorkDistributionChanges changes =
                                consumerAssignmentRegistry.updateAssignments(initialState, work.getAssignmentsView());

                        logger.info("Finished workload balance {}, {}", work.toString(), changes.toString());

                        clusterAssignmentCache.refresh(); // refresh cache with just stored data

                        updateMetrics(work, changes);
                    } else {
                        logger.info("Lost leadership before applying changes");
                    }
                }
            } else {
                balancingMetrics.reset();
            }
        } catch (Exception e) {
            logger.error("Caught exception when running balancing job", e);
        }
    }

    private void log(List<SubscriptionName> subscriptions, List<String> activeConsumerNodes,
                     SubscriptionAssignmentView currentState, SubscriptionAssignmentView balancedState) {
        logger.info("Balancing {} subscriptions across {} nodes with previous {} assignments" +
                        " produced {} assignments",
                subscriptions.size(),
                activeConsumerNodes.size(),
                currentState.getAllAssignments().size(),
                balancedState.getAllAssignments().size());
    }

    private int countMissingResources(List<SubscriptionName> subscriptions, SubscriptionAssignmentView state, WorkloadConstraints constraints) {
        return subscriptions.stream()
                .mapToInt(s -> {
                    int requiredConsumers = constraints.getConsumersNumber(s);
                    int subscriptionAssignments = state.getAssignmentsCountForSubscription(s);
                    int missing = requiredConsumers - subscriptionAssignments;
                    if (missing != 0) {
                        logger.info("Subscription {} has {} != {} (default) assignments",
                                s, subscriptionAssignments, requiredConsumers);
                    }
                    return missing;
                })
                .sum();
    }

    private List<SubscriptionName> findRemovedSubscriptions(SubscriptionAssignmentView state,
                                                            List<SubscriptionName> subscriptions) {
        return state.getSubscriptions().stream().filter(s -> !subscriptions.contains(s)).collect(toList());
    }

    private List<String> findInactiveConsumers(SubscriptionAssignmentView state, List<String> activeConsumers) {
        return state.getConsumerNodes().stream().filter(c -> !activeConsumers.contains(c)).collect(toList());
    }

    private List<SubscriptionName> findNewSubscriptions(SubscriptionAssignmentView state,
                                                        List<SubscriptionName> subscriptions) {
        return subscriptions.stream().filter(s -> !state.getSubscriptions().contains(s)).collect(toList());
    }

    private List<String> findNewConsumers(SubscriptionAssignmentView state, List<String> activeConsumers) {
        return activeConsumers.stream().filter(c -> !state.getConsumerNodes().contains(c)).collect(toList());
    }

    void start() {
        job = executorService.scheduleAtFixedRate(this, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    }

    void stop() throws InterruptedException {
        job.cancel(false);
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
    }

    private void updateMetrics(WorkBalancingResult balancingResult, WorkDistributionChanges changes) {
        this.balancingMetrics.allAssignments = balancingResult.getAssignmentsView().getAllAssignments().size();
        this.balancingMetrics.missingResources = balancingResult.getMissingResources();
        this.balancingMetrics.createdAssignments = changes.getCreatedAssignmentsCount();
        this.balancingMetrics.deletedAssignments = changes.getDeletedAssignmentsCount();
    }

    private static class BalancingJobMetrics {

        volatile int allAssignments;

        volatile int missingResources;

        volatile int deletedAssignments;

        volatile int createdAssignments;

        void reset() {
            this.allAssignments = 0;
            this.missingResources = 0;
            this.deletedAssignments = 0;
            this.createdAssignments = 0;
        }
    }
}
