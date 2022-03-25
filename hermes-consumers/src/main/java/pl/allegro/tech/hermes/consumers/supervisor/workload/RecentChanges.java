package pl.allegro.tech.hermes.consumers.supervisor.workload;

import pl.allegro.tech.hermes.api.SubscriptionName;

import java.util.List;

public class RecentChanges {
    private final List<SubscriptionName> removedSubscriptions;
    private final List<String> inactiveConsumers;
    private final List<SubscriptionName> newSubscriptions;
    private final List<String> newConsumers;

    public RecentChanges(List<SubscriptionName> removedSubscriptions,
                         List<String> inactiveConsumers,
                         List<SubscriptionName> newSubscriptions,
                         List<String> newConsumers) {
        this.removedSubscriptions = removedSubscriptions;
        this.inactiveConsumers = inactiveConsumers;
        this.newSubscriptions = newSubscriptions;
        this.newConsumers = newConsumers;
    }

    public List<SubscriptionName> getRemovedSubscriptions() {
        return removedSubscriptions;
    }

    public List<String> getInactiveConsumers() {
        return inactiveConsumers;
    }

    public List<SubscriptionName> getNewSubscriptions() {
        return newSubscriptions;
    }

    public List<String> getNewConsumers() {
        return newConsumers;
    }

    public boolean nothingChanged() {
        return removedSubscriptions.isEmpty() && inactiveConsumers.isEmpty() && newSubscriptions.isEmpty() && newConsumers.isEmpty();
    }
}
