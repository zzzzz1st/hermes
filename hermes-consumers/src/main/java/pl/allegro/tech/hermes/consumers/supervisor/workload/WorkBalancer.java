package pl.allegro.tech.hermes.consumers.supervisor.workload;

import pl.allegro.tech.hermes.domain.workload.constraints.WorkloadConstraints;

public interface WorkBalancer {

    SubscriptionAssignmentView balance(RecentChanges recentChanges,
                                       SubscriptionAssignmentView currentState,
                                       WorkloadConstraints constraints);
}
