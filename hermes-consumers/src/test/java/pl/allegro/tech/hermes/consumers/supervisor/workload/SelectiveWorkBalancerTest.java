package pl.allegro.tech.hermes.consumers.supervisor.workload;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.assertj.core.api.ListAssert;
import org.junit.Test;
import org.junit.runner.RunWith;
import pl.allegro.tech.hermes.api.Constraints;
import pl.allegro.tech.hermes.api.SubscriptionName;
import pl.allegro.tech.hermes.consumers.supervisor.workload.selective.SelectiveWorkBalancer;
import pl.allegro.tech.hermes.domain.workload.constraints.WorkloadConstraints;

import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(DataProviderRunner.class)
public class SelectiveWorkBalancerTest {

    private static final int CONSUMERS_PER_SUBSCRIPTION = 2;
    private static final int MAX_SUBSCRIPTIONS_PER_CONSUMER = 2;

    private final SelectiveWorkBalancer workBalancer = new SelectiveWorkBalancer();

    @Test
    public void shouldPerformSubscriptionsCleanup() {
        // given
        List<SubscriptionName> subscriptions = someSubscriptions(1);
        List<String> supervisors = someSupervisors(1);
        WorkloadConstraints constraints = WorkloadConstraints
                .defaultConstraints(CONSUMERS_PER_SUBSCRIPTION, MAX_SUBSCRIPTIONS_PER_CONSUMER, supervisors.size());
        SubscriptionAssignmentView currentState = initialState(subscriptions, supervisors, constraints);
        RecentChanges recentChanges = new RecentChanges(
                subscriptions,
                emptyList(),
                emptyList(),
                emptyList()
        );

        // when
        SubscriptionAssignmentView balancedState = workBalancer.balance(recentChanges, currentState, constraints);

        // then
        assertThat(balancedState.getSubscriptions()).isEmpty();
        assertThat(balancedState.getConsumerNodes()).isEmpty();
    }

    @Test
    public void shouldPerformSupervisorsCleanup() {
        // given
        List<String> supervisors = someSupervisors(2);
        List<SubscriptionName> subscriptions = someSubscriptions(1);
        WorkloadConstraints constraints = WorkloadConstraints
                .defaultConstraints(CONSUMERS_PER_SUBSCRIPTION, MAX_SUBSCRIPTIONS_PER_CONSUMER, supervisors.size());
        SubscriptionAssignmentView currentState = initialState(subscriptions, supervisors, constraints);
        RecentChanges recentChanges = new RecentChanges(
                emptyList(),
                singletonList(supervisors.get(1)),
                emptyList(),
                emptyList()
        );

        // when
        SubscriptionAssignmentView balancedState = workBalancer.balance(recentChanges, currentState, constraints);

        // then
        assertThat(balancedState.getConsumerNodes()).hasSize(1);
        assertThatSubscriptionIsAssignedTo(balancedState, subscriptions.get(0), supervisors.get(0));
    }

    @Test
    public void shouldBalanceWorkForSingleSubscription() {
        // given
        List<String> supervisors = someSupervisors(1);
        List<SubscriptionName> subscriptions = someSubscriptions(1);
        WorkloadConstraints constraints = WorkloadConstraints
                .defaultConstraints(CONSUMERS_PER_SUBSCRIPTION, MAX_SUBSCRIPTIONS_PER_CONSUMER, supervisors.size());

        // when
        SubscriptionAssignmentView view = initialState(subscriptions, supervisors, constraints);

        // then
        assertThatSubscriptionIsAssignedTo(view, subscriptions.get(0), supervisors.get(0));
    }

    @Test
    public void shouldBalanceWorkForMultipleConsumersAndSingleSubscription() {
        // given
        List<String> supervisors = someSupervisors(2);
        List<SubscriptionName> subscriptions = someSubscriptions(1);
        WorkloadConstraints constraints = WorkloadConstraints
                .defaultConstraints(CONSUMERS_PER_SUBSCRIPTION, MAX_SUBSCRIPTIONS_PER_CONSUMER, supervisors.size());

        // when
        SubscriptionAssignmentView view = initialState(subscriptions, supervisors, constraints);

        // then
        assertThatSubscriptionIsAssignedTo(view, subscriptions.get(0), supervisors);
    }

    @Test
    public void shouldBalanceWorkForMultipleConsumersAndMultipleSubscriptions() {
        // given
        List<String> supervisors = someSupervisors(2);
        List<SubscriptionName> subscriptions = someSubscriptions(2);
        WorkloadConstraints constraints = WorkloadConstraints
                .defaultConstraints(CONSUMERS_PER_SUBSCRIPTION, MAX_SUBSCRIPTIONS_PER_CONSUMER, supervisors.size());

        // when
        SubscriptionAssignmentView view = initialState(subscriptions, supervisors, constraints);

        // then
        assertThatSubscriptionIsAssignedTo(view, subscriptions.get(0), supervisors);
        assertThatSubscriptionIsAssignedTo(view, subscriptions.get(1), supervisors);
    }

    @Test
    public void shouldNotOverloadConsumers() {
        // given
        List<String> supervisors = someSupervisors(1);
        List<SubscriptionName> subscriptions = someSubscriptions(3);
        WorkloadConstraints constraints = WorkloadConstraints
                .defaultConstraints(CONSUMERS_PER_SUBSCRIPTION, MAX_SUBSCRIPTIONS_PER_CONSUMER, supervisors.size());

        // when
        SubscriptionAssignmentView view = initialState(subscriptions, supervisors, constraints);

        // then
        assertThat(view.getAssignmentsForConsumerNode(supervisors.get(0))).hasSize(2);
    }

    @Test
    public void shouldRebalanceAfterConsumerDisappearing() {
        // given
        List<String> supervisors = ImmutableList.of("c1", "c2");
        List<SubscriptionName> subscriptions = someSubscriptions(2);
        WorkloadConstraints constraints = WorkloadConstraints
                .defaultConstraints(CONSUMERS_PER_SUBSCRIPTION, MAX_SUBSCRIPTIONS_PER_CONSUMER, supervisors.size());
        SubscriptionAssignmentView currentState = initialState(subscriptions, supervisors, constraints);
        List<String> extendedSupervisorsList = ImmutableList.of("c3");
        RecentChanges recentChanges = new RecentChanges(
                emptyList(),
                ImmutableList.of("c2"),
                emptyList(),
                extendedSupervisorsList
        );

        // when
        SubscriptionAssignmentView stateAfterRebalance = workBalancer.balance(recentChanges, currentState, constraints);

        // then
        assertThat(stateAfterRebalance.getSubscriptionsForConsumerNode("c3"))
                .containsOnly(subscriptions.get(0), subscriptions.get(1));
    }

    @Test
    public void shouldAssignWorkToNewConsumersByWorkStealing() {
        // given
        List<String> supervisors = someSupervisors(2);
        List<SubscriptionName> subscriptions = someSubscriptions(2);
        WorkloadConstraints initialConstraints = WorkloadConstraints
                .defaultConstraints(CONSUMERS_PER_SUBSCRIPTION, MAX_SUBSCRIPTIONS_PER_CONSUMER, supervisors.size());
        SubscriptionAssignmentView currentState = initialState(subscriptions, supervisors, initialConstraints);
        WorkloadConstraints constraints = WorkloadConstraints
                .defaultConstraints(CONSUMERS_PER_SUBSCRIPTION, MAX_SUBSCRIPTIONS_PER_CONSUMER, supervisors.size());
        RecentChanges recentChanges = new RecentChanges(
                emptyList(),
                emptyList(),
                emptyList(),
                ImmutableList.of("new-supervisor")
        );

        // when
        SubscriptionAssignmentView stateAfterRebalance = workBalancer.balance(recentChanges, currentState, constraints);

        // then
        assertThat(stateAfterRebalance.getAssignmentsForConsumerNode("new-supervisor").size()).isGreaterThan(0);
        assertThat(stateAfterRebalance.getAssignmentsForSubscription(subscriptions.get(0))).hasSize(2);
        assertThat(stateAfterRebalance.getAssignmentsForSubscription(subscriptions.get(1))).hasSize(2);
    }

    @Test
    public void shouldEquallyAssignWorkToConsumers() {
        // given
        List<String> supervisors = ImmutableList.of("c1", "c2");
        List<SubscriptionName> subscriptions = someSubscriptions(50);
        WorkloadConstraints initialConstraints = WorkloadConstraints.defaultConstraints(2, 200, supervisors.size());
        SubscriptionAssignmentView currentState = initialState(subscriptions, supervisors, initialConstraints);
        WorkloadConstraints constraints = WorkloadConstraints.defaultConstraints(2, 200, supervisors.size());
        List<String> extendedSupervisorsList = ImmutableList.of("c3");
        RecentChanges recentChanges = new RecentChanges(
                emptyList(),
                emptyList(),
                emptyList(),
                extendedSupervisorsList
        );

        // when
        SubscriptionAssignmentView stateAfterRebalance = workBalancer.balance(recentChanges, currentState, constraints);

        // then
        assertThat(stateAfterRebalance.getAssignmentsForConsumerNode("c3")).hasSize(50 * 2 / 3);
    }

    @Test
    public void shouldReassignWorkToFreeConsumers() {
        // given
        List<String> supervisors = ImmutableList.of("c1");
        List<SubscriptionName> subscriptions = someSubscriptions(10);
        WorkloadConstraints initialConstraints = WorkloadConstraints.defaultConstraints(1, 100, supervisors.size());
        SubscriptionAssignmentView currentState = initialState(subscriptions, supervisors, initialConstraints);
        WorkloadConstraints constraints = WorkloadConstraints.defaultConstraints(1, 100, supervisors.size());
        List<String> extendedSupervisorsList = ImmutableList.of("c2", "c3", "c4", "c5");
        RecentChanges recentChanges = new RecentChanges(
                emptyList(),
                emptyList(),
                emptyList(),
                extendedSupervisorsList
        );

        // when
        SubscriptionAssignmentView stateAfterRebalance = workBalancer.balance(recentChanges, currentState, constraints);

        // then
        assertThat(stateAfterRebalance.getAssignmentsForConsumerNode("c5")).hasSize(2);
    }

    @Test
    public void shouldRemoveRedundantWorkAssignmentsToKeepWorkloadMinimal() {
        // given
        List<String> supervisors = ImmutableList.of("c1", "c2", "c3");
        List<SubscriptionName> subscriptions = someSubscriptions(10);
        WorkloadConstraints initialConstraints = WorkloadConstraints.defaultConstraints(3, 100, supervisors.size());
        SubscriptionAssignmentView currentState = initialState(subscriptions, supervisors, initialConstraints);
        WorkloadConstraints newConstraints = WorkloadConstraints.defaultConstraints(1, 100, supervisors.size());
        RecentChanges recentChanges = new RecentChanges(
                emptyList(),
                emptyList(),
                emptyList(),
                emptyList()
        );

        // when
        SubscriptionAssignmentView stateAfterRebalance = workBalancer.balance(recentChanges, currentState, newConstraints);

        // then
        assertThat(stateAfterRebalance.getAssignmentsCountForSubscription(subscriptions.get(0))).isEqualTo(1);
    }

    @DataProvider
    public static Object[][] subscriptionConstraints() {
        return new Object[][] {
                { 1 }, { 3 }
        };
    }

    @Test
    @UseDataProvider("subscriptionConstraints")
    public void shouldAssignConsumersForSubscriptionsAccordingToConstraints(int requiredConsumersNumber) {
        // given
        SubscriptionAssignmentView initialState = new SubscriptionAssignmentView(emptyMap());

        List<String> supervisors = ImmutableList.of("c1", "c2", "c3");
        List<SubscriptionName> subscriptions = someSubscriptions(4);
        WorkloadConstraints subscriptionConstraints = new WorkloadConstraints(ImmutableMap.of(
                subscriptions.get(0), new Constraints(requiredConsumersNumber)
        ), emptyMap(), 2, 4, supervisors.size());
        RecentChanges recentChanges = new RecentChanges(
                emptyList(),
                emptyList(),
                subscriptions,
                supervisors
        );

        // when
        SubscriptionAssignmentView state = workBalancer.balance(recentChanges, initialState, subscriptionConstraints);

        // then
        assertThat(state.getAssignmentsForSubscription(subscriptions.get(0)).size()).isEqualTo(requiredConsumersNumber);
        assertThat(state.getAssignmentsForSubscription(subscriptions.get(1)).size()).isEqualTo(2);
        assertThat(state.getAssignmentsForSubscription(subscriptions.get(2)).size()).isEqualTo(2);
        assertThat(state.getAssignmentsForSubscription(subscriptions.get(3)).size()).isEqualTo(2);
    }

    private SubscriptionAssignmentView initialState(List<SubscriptionName> subscriptions, List<String> supervisors,
                                                    WorkloadConstraints workloadConstraints) {
        RecentChanges recentChanges = new RecentChanges(
                emptyList(),
                emptyList(),
                subscriptions,
                supervisors
        );
        return workBalancer.balance(recentChanges, new SubscriptionAssignmentView(emptyMap()), workloadConstraints);
    }

    private List<SubscriptionName> someSubscriptions(int count) {
        return IntStream.range(0, count).mapToObj(i -> anySubscription()).collect(toList());
    }

    private List<String> someSupervisors(int count) {
        return IntStream.range(0, count).mapToObj(i -> "c" + i).collect(toList());
    }

    private SubscriptionName anySubscription() {
        return SubscriptionName.fromString("tech.topic$s" + UUID.randomUUID().getMostSignificantBits());
    }

    private ListAssert<String> assertThatSubscriptionIsAssignedTo(SubscriptionAssignmentView work, SubscriptionName sub, String... nodeIds) {
        return assertThatSubscriptionIsAssignedTo(work, sub, asList(nodeIds));
    }

    private ListAssert<String> assertThatSubscriptionIsAssignedTo(SubscriptionAssignmentView work, SubscriptionName sub, List<String> nodeIds) {
        return assertThat(work.getAssignmentsForSubscription(sub))
                .extracting(SubscriptionAssignment::getConsumerNodeId).containsOnly(nodeIds.stream().toArray(String[]::new));
    }
}
