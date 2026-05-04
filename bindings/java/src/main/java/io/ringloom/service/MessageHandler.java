package io.ringloom.service;

/**
 * Callback for messages delivered by {@link MessageConsumer#poll(MessageHandler, int)}.
 */
@FunctionalInterface
public interface MessageHandler {
    /**
     * Handles one borrowed message view.
     *
     * @param message reusable message view valid until the callback returns
     */
    void onMessage(RingloomMessage message);
}
