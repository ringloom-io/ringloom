package io.ringloom.service;

/**
 * Callback for service discovery events.
 *
 * <p>Handlers run synchronously on the application thread that calls
 * {@link RingloomService#pollControl(int)}.</p>
 */
@FunctionalInterface
public interface ServiceLifecycleHandler {
    /**
     * Handles one lifecycle event.
     *
     * @param event borrowed event data decoded from the native callback
     */
    void onServiceLifecycle(ServiceLifecycleEvent event);
}
