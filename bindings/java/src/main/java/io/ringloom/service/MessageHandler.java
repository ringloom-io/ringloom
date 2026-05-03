package io.ringloom.service;

@FunctionalInterface
public interface MessageHandler {
    void onMessage(RingloomMessage message);
}
