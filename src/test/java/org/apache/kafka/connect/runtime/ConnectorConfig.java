package org.apache.kafka.connect.runtime;

/**
 * Test-only stand-in, present specifically to prove a NEGATIVE: ConnectorConfig sits in the same
 * package as WorkerSinkTask and its toString would dump connector configuration including
 * credentials, so the allowlist must be exact class names and never a package prefix.
 */
public final class ConnectorConfig {
    @Override
    public String toString() {
        return "connection.password=hunter2, connection.user=jo.tan@example.com";
    }
}
