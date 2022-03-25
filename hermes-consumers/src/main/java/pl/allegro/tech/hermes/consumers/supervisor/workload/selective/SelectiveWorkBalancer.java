package pl.allegro.tech.hermes.consumers.supervisor.workload.selective;

import pl.allegro.tech.hermes.api.SubscriptionName;
import pl.allegro.tech.hermes.consumers.supervisor.workload.RecentChanges;
import pl.allegro.tech.hermes.consumers.supervisor.workload.SubscriptionAssignment;
import pl.allegro.tech.hermes.consumers.supervisor.workload.SubscriptionAssignmentView;
import pl.allegro.tech.hermes.consumers.supervisor.workload.WorkBalancer;
import pl.allegro.tech.hermes.domain.workload.constraints.WorkloadConstraints;

import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Comparator.comparingInt;

public class SelectiveWorkBalancer implements WorkBalancer {

    @Override
    public SubscriptionAssignmentView balance(RecentChanges recentChanges,
                                              SubscriptionAssignmentView currentState,
                                              WorkloadConstraints constraints) {
        return currentState.transform((state, transformer) -> {
            recentChanges.getRemovedSubscriptions().forEach(transformer::removeSubscription);
            recentChanges.getInactiveConsumers().forEach(transformer::removeConsumerNode);
            recentChanges.getNewSubscriptions().forEach(transformer::addSubscription);
            recentChanges.getNewConsumers().forEach(transformer::addConsumerNode);
            minimizeWorkload(state, transformer, constraints);
            AvailableWork.stream(state, constraints)
                    .forEach(transformer::addAssignment);
            equalizeWorkload(state, transformer);
        });
    }

    private void minimizeWorkload(SubscriptionAssignmentView state,
                                  SubscriptionAssignmentView.Transformer transformer,
                                  WorkloadConstraints workloadConstraints) {
        state.getSubscriptions()
                .stream()
                .flatMap(subscriptionName -> findRedundantAssignments(state, subscriptionName, workloadConstraints))
                .forEach(transformer::removeAssignment);
    }

    private Stream<SubscriptionAssignment> findRedundantAssignments(SubscriptionAssignmentView state,
                                                                    SubscriptionName subscriptionName,
                                                                    WorkloadConstraints constraints) {
        final int assignedConsumers = state.getAssignmentsCountForSubscription(subscriptionName);
        final int requiredConsumers = constraints.getConsumersNumber(subscriptionName);
        int redundantConsumers = assignedConsumers - requiredConsumers;
        if (redundantConsumers > 0) {
            Stream.Builder<SubscriptionAssignment> redundant = Stream.builder();
            Iterator<SubscriptionAssignment> iterator = state.getAssignmentsForSubscription(subscriptionName).iterator();
            while (redundantConsumers > 0 && iterator.hasNext()) {
                SubscriptionAssignment assignment = iterator.next();
                redundant.add(assignment);
                redundantConsumers--;
            }
            return redundant.build();
        }
        return Stream.empty();
    }

    private void equalizeWorkload(SubscriptionAssignmentView state,
                                  SubscriptionAssignmentView.Transformer transformer) {
        if (state.getSubscriptionsCount() > 1 && !state.getConsumerNodes().isEmpty()) {
            boolean transferred;
            do {
                transferred = false;

                String maxLoaded = maxLoadedConsumerNode(state);
                String minLoaded = minLoadedConsumerNode(state);
                int maxLoad = state.getAssignmentsCountForConsumerNode(maxLoaded);
                int minLoad = state.getAssignmentsCountForConsumerNode(minLoaded);

                while (maxLoad > minLoad + 1) {
                    Optional<SubscriptionName> subscription = getSubscriptionForTransfer(state, maxLoaded, minLoaded);
                    if (subscription.isPresent()) {
                        transformer.transferAssignment(maxLoaded, minLoaded, subscription.get());
                        transferred = true;
                    } else break;
                    maxLoad--;
                    minLoad++;
                }
            } while (transferred);
        }
    }

    private String maxLoadedConsumerNode(SubscriptionAssignmentView state) {
        return state.getConsumerNodes().stream().max(comparingInt(state::getAssignmentsCountForConsumerNode)).get();
    }

    private String minLoadedConsumerNode(SubscriptionAssignmentView state) {
        return state.getConsumerNodes().stream().min(comparingInt(state::getAssignmentsCountForConsumerNode)).get();
    }

    private Optional<SubscriptionName> getSubscriptionForTransfer(SubscriptionAssignmentView state,
                                                                  String maxLoaded,
                                                                  String minLoaded) {
        return state.getSubscriptionsForConsumerNode(maxLoaded).stream()
                .filter(s -> !state.getConsumerNodesForSubscription(s).contains(minLoaded))
                .findAny();
    }
}
