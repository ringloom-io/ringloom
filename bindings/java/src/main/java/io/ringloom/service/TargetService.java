package io.ringloom.service;

public record TargetService(
    int targetServiceId,
    boolean leader
) {
}
