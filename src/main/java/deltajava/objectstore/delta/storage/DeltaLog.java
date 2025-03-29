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
     * Currently loaded snapshot
     */
    private Snapshot currentSnapshot;

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
        
        // Initialize with an empty snapshot
        try {
            this.currentSnapshot = new Snapshot(this, -1, Collections.emptyList());
        } catch (Exception e) {
            // Ignore and keep null snapshot, it will be initialized on first access
        }
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

    public OptimisticTransaction startTransaction() throws IOException {
        return new OptimisticTransaction(storage, tablePath);
    }

    public void write(long version, List<Action> actions) throws IOException {
        if (version < 0) {
            throw new IllegalArgumentException("Version number cannot be negative: " + version);
        }
        String logFileName = LogFileName.fromVersion(version).getPathIn(logPath);
        storage.writeObject(logFileName, JsonUtil.toJson(actions).getBytes());
    }

    public Snapshot snapshot() throws IOException {
        long latestVersion = getLatestVersion();
        if (latestVersion < 0) {
            // Empty snapshot for empty log
            return new Snapshot(this, -1, Collections.emptyList());
        }
        
        List<Action> allActions = new ArrayList<>();
        
        // Read actions from all versions to construct complete state
        List<Long> versions = listVersions();
        Collections.sort(versions); // Process versions in order
        
        for (Long version : versions) {
            Collection<Action> versionActions = readVersion(version);
            allActions.addAll(versionActions);
        }
        
        return new Snapshot(this, latestVersion, allActions);
    }

    public Snapshot update() throws IOException {
        try {
            deltaLogLock.lock();

            // Get the latest version
            long latestVersion = getLatestVersion();

            // If no change or no log files exist yet, return current snapshot
            if (currentSnapshot != null && latestVersion == currentSnapshot.getVersion()) {
                return currentSnapshot;
            }

            // Create a new snapshot with the complete state
            currentSnapshot = snapshot();
            return currentSnapshot;
        } finally {
            deltaLogLock.unlock();
        }
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

    public Collection<Action> readVersion(Long version) throws IOException {
        if (version == null || version < 0) {
            throw new IllegalArgumentException("Version must be a non-negative number");
        }
        
        String logFileName = LogFileName.fromVersion(version).getPathIn(logPath);
        
        try {
            byte[] data = storage.readObject(logFileName);
            String json = new String(data);
            return JsonUtil.jsonToActions(json);
        } catch (IOException e) {
            throw new IOException("Failed to read version " + version, e);
        }
    }

    public void lock() {
        deltaLogLock.lock();
    }

    public void releaseLock() {
        if (deltaLogLock.isHeldByCurrentThread()) {
            deltaLogLock.unlock();
        }
    }
}
