package io.ringloom.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

final class TestBroker implements AutoCloseable {
    private final Path repoRoot;
    private final Path workspace;
    private final short nodeId;
    private final String storagePath;
    private final String group;
    private boolean closed;
    private Process directProcess;

    private TestBroker(Path repoRoot, Path workspace, short nodeId, String storagePath, String group) {
        this.repoRoot = repoRoot;
        this.workspace = workspace;
        this.nodeId = nodeId;
        this.storagePath = storagePath;
        this.group = group;
    }

    static TestBroker startKvEnabled(Path repoRoot, Path workspace) throws IOException, InterruptedException {
        java.nio.file.Files.createDirectories(workspace);
        Path storage=workspace.resolve("storage");
        Path config=workspace.resolve("kv-broker.properties");
        Path log=workspace.resolve("kv-broker.log");
        java.nio.file.Files.writeString(config,"broker.node.id=1\nbroker.local.host.port=127.0.0.1:19001\nbroker.group.name=ringloom-java-test\nbroker.storage.path="+storage+"\nbroker.topics.enabled=true\nbroker.kv.enabled=true\nbroker.threading.mode=dedicated\n");
        Process process=new ProcessBuilder(System.getProperty("ringloom.brokerBin"),"--config",config.toString()).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        long deadline=System.nanoTime()+10_000_000_000L;
        while(System.nanoTime()<deadline && process.isAlive()) {
            if(java.nio.file.Files.readString(log).contains("broker started:")) {
                var broker=new TestBroker(repoRoot,workspace,(short)1,storage.toString(),"ringloom-java-test");broker.directProcess=process;return broker;
            }
            Thread.sleep(25);
        }
        process.destroyForcibly();process.waitFor();throw new IOException("KV broker failed: "+java.nio.file.Files.readString(log));
    }

    static TestBroker start(Path repoRoot, Path workspace) throws IOException, InterruptedException {
        return start(repoRoot, workspace, (short) 1, 19001, "ringloom-java-test");
    }

    /** Single-node topics-enabled broker, for topic integration tests. */
    static TestBroker startTopicsEnabled(Path repoRoot, Path workspace) throws IOException, InterruptedException {
        return start(repoRoot, workspace, (short) 1, 19001, "ringloom-java-test", new String[0], true);
    }

    static TestBroker start(
        Path repoRoot,
        Path workspace,
        short nodeId,
        int port,
        String group,
        String[] peers,
        boolean topicsEnabled
    ) throws IOException, InterruptedException {
        Path brokerBin = Path.of(System.getProperty("ringloom.brokerBin"));
        var command = new java.util.ArrayList<String>();
        command.add("bash");
        command.add(repoRoot.resolve("scripts/start-test-broker.sh").toString());
        command.add("--workspace");
        command.add(workspace.toString());
        command.add("--node-id");
        command.add(Short.toString(nodeId));
        command.add("--port");
        command.add(Integer.toString(port));
        command.add("--group");
        command.add(group);
        command.add("--daemon");
        command.add("--bin-dir");
        command.add(brokerBin.getParent().toString());
        if (topicsEnabled) {
            command.add("--topics");
        }
        for (String peer : peers) {
            command.add("--peer");
            command.add(peer);
        }

        Process process = new ProcessBuilder(command).directory(repoRoot.toFile()).redirectErrorStream(true).start();

        String output = new String(process.getInputStream().readAllBytes());
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new IOException("failed to start test broker:\n" + output);
        }

        Map<String, String> env = parseEnv(output);
        return new TestBroker(
            repoRoot,
            workspace,
            Short.parseShort(env.get("RINGLOOM_BROKER_NODE_ID")),
            env.get("RINGLOOM_STORAGE_PATH"),
            env.get("RINGLOOM_GROUP")
        );
    }

    /** Backwards-compatible overload: single-node, topics disabled. */
    static TestBroker start(
        Path repoRoot,
        Path workspace,
        short nodeId,
        int port,
        String group,
        String... peers
    ) throws IOException, InterruptedException {
        return start(repoRoot, workspace, nodeId, port, group, peers, false);
    }

    short nodeId() {
        return nodeId;
    }

    String storagePath() {
        return storagePath;
    }

    String group() {
        return group;
    }

    @Override
    public void close() throws IOException, InterruptedException {
        if (closed) {
            return;
        }
        closed = true;

        if(directProcess!=null) {
            directProcess.destroy();
            if(!directProcess.waitFor(10,java.util.concurrent.TimeUnit.SECONDS)) {directProcess.destroyForcibly();directProcess.waitFor();}
            return;
        }
        Process process = new ProcessBuilder(
            "bash",
            repoRoot.resolve("scripts/start-test-broker.sh").toString(),
            "--workspace", workspace.toString(),
            "--node-id", Short.toString(nodeId),
            "--stop"
        ).directory(repoRoot.toFile()).redirectErrorStream(true).start();

        String output = new String(process.getInputStream().readAllBytes());
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new IOException("failed to stop test broker:\n" + output);
        }
    }

    private static Map<String, String> parseEnv(String output) throws IOException {
        Map<String, String> env = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new StringReader(output))) {
            String line;
            while ((line = reader.readLine()) != null) {
                int equals = line.indexOf('=');
                if (equals <= 0) {
                    continue;
                }
                env.put(line.substring(0, equals), line.substring(equals + 1));
            }
        }

        if (!env.containsKey("RINGLOOM_STORAGE_PATH") || !env.containsKey("RINGLOOM_GROUP")) {
            throw new IOException("broker script did not emit expected environment variables:\n" + output);
        }
        return env;
    }
}
