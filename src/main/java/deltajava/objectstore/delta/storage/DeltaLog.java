package deltajava.objectstore.delta.storage;

import deltajava.objectstore.delta.storage.actions.Action;
import deltajava.objectstore.delta.storage.util.JsonUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class DeltaLog {

    /**
     * Path to the table's log directory
     */
    private final String logPath;

    /**
     * Path to the table's data directory
     */
    private final String dataPath;

    /**
     * Lock for coordination of concurrent operations
     */
    private final ReentrantLock deltaLogLock = new ReentrantLock();
    private final Storage storage;
    private final String tablePath;


    /**
     * Creates a DeltaLog for the specified table path.
     *
     * @param tablePath the path to the Delta table
     */
    public DeltaLog(Storage storage, String tablePath) {
        this.storage = storage;
        this.tablePath = tablePath;
        if (!tablePath.endsWith("/")) {
            tablePath += "/";
        }
        this.logPath = tablePath + "_delta_log/";
        this.dataPath = tablePath + "data/";
    }

    public String getLogPath() {
        return logPath;
    }

    public String getDataPath() {
        return dataPath;
    }

    public long getLatestVersion() throws IOException {
        List<Long> versions = listVersions();
        
        if (versions.isEmpty()) {
            return -1;
        }
        
        return Collections.max(versions);
    }

    public OptimisticTransaction startTransaction() {
        return new OptimisticTransaction(storage, tablePath);
    }

    public void write(long version, List<Action> actions) throws IOException {
        if (version < 0) {
            throw new IllegalArgumentException("Version number cannot be negative: " + version);
        }
        String logFileName = LogFileName.fromVersion(version).getPathIn(logPath);
        storage.writeObject(logFileName, JsonUtil.toJson(actions).getBytes());
    }

    public Snapshot update() {
        return null;
    }

    public List<Long> listVersions() {
        try {
            List<String> logFiles = storage.listObjects(logPath);
            return extractVersions(logFiles);

        } catch (IOException e) {
            // Log the error and return empty list
            return Collections.emptyList();
        }
    }

    private static List<Long> extractVersions(List<String> logFiles) {
        List<Long> versions = new ArrayList<>();

        for (String filePath : logFiles) {
            long version = LogFileName.versionFromName(filePath);
            if (version >= 0) {
                versions.add(version);
            }
        }

        return versions;
    }

    public Collection<? extends Action> readVersion(Long version) {
        return null;
    }

    public Snapshot snapshot() {
        return null;
    }

    public void lock() {

    }

    public void releaseLock() {

    }
}
