package io.ringloom.service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

final class TestSupport {
    private TestSupport() {
    }

    static Path repoRoot() {
        String projectRoot = System.getProperty("ringloom.projectRoot");
        if (projectRoot == null || projectRoot.isBlank()) {
            throw new IllegalStateException("ringloom.projectRoot system property is required");
        }
        return Path.of(projectRoot);
    }

    static Path createWorkspace(String prefix) throws IOException {
        return Files.createTempDirectory(prefix);
    }

    static void cleanupWorkspace(Path workspace, boolean success) throws IOException {
        if (!success) {
            System.err.println("Preserving RingLoom Java workspace: " + workspace);
            return;
        }

        try (var stream = Files.walk(workspace)) {
            stream.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            });
        } catch (RuntimeException ex) {
            if (ex.getCause() instanceof IOException ioException) {
                throw ioException;
            }
            throw ex;
        }
    }

    static ServiceConfig serviceConfig(String serviceName, TestBroker broker) {
        return new ServiceConfig(
            serviceName,
            broker.storagePath(),
            broker.group(),
            (short) 1,
            false,
            10_000,
            65_536L,
            1_048_576L,
            false
        );
    }
}
