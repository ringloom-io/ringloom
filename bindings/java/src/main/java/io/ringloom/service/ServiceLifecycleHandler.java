package io.ringloom.service;

@FunctionalInterface
public interface ServiceLifecycleHandler {
    void onServiceLifecycle(ServiceLifecycleEvent event);
}
