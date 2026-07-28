package org.apache.kafka.connect.runtime;

/** Test-only stand-in; see org.apache.kafka.connect.util.ConnectorTaskId for why this exists. */
public final class WorkerSinkTask {
    private final String id;

    public WorkerSinkTask(final String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "WorkerSinkTask{id=" + id + "}";
    }
}
