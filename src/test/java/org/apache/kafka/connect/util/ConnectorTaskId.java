package org.apache.kafka.connect.util;

/**
 * Test-only stand-in for Connect's ConnectorTaskId.
 *
 * PiiRedactionPolicy allowlists identifier arguments by fully-qualified class NAME, not by
 * instanceof, so that the shipped jar needs nothing beyond log4j-core on the Connect worker
 * classpath. The real class lives in connect-runtime, which this repo deliberately does not depend
 * on - so the only way to exercise the real matching path is a stub carrying the real FQCN.
 *
 * If a future change adds a connect-runtime dependency, delete this and use the real class.
 */
public final class ConnectorTaskId {
    private final String connector;
    private final int task;

    public ConnectorTaskId(final String connector, final int task) {
        this.connector = connector;
        this.task = task;
    }

    @Override
    public String toString() {
        return connector + "-" + task;
    }
}
