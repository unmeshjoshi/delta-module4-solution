package deltajava.objectstore.delta.storage;

import deltajava.objectstore.delta.storage.actions.Action;
import deltajava.objectstore.delta.storage.actions.AddFile;
import deltajava.objectstore.delta.storage.actions.CommitInfo;
import deltajava.objectstore.delta.storage.util.ParquetUtil;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * An implementation of optimistic concurrency control for transactions.
 * Handles conflicts between concurrent operations using different isolation levels.
 */
public class OptimisticTransaction extends Transaction {

    private final Snapshot snapshot;

    /**
     * Isolation level that determines how conflicts are detected and resolved.
     */
    public enum IsolationLevel {
        /**
         * Serializable ensures that the transaction appears to have occurred
         * at a single point in time, with no concurrent transactions.
         */
        SERIALIZABLE,

        /**
         * WriteSerializable ensures that only write operations cause conflicts,
         * allowing concurrent reads.
         */
        WRITE_SERIALIZABLE
    }

    private final Storage storage;
    private final IsolationLevel isolationLevel;
    private final long readVersion;
    private final Map<String, Map<String, String>> newMetadata;
    private final Set<String> readPredicates;
    private final AtomicInteger retryCount;
    private final int maxRetryCount;
    private final DeltaLog deltaLog;

    /**
     * Creates a new optimistic transaction with default isolation level.
     *
     * @param tablePath the path to the Delta table
     */
    public OptimisticTransaction(Storage storage, String tablePath) throws IOException {
        this(storage, tablePath, IsolationLevel.SERIALIZABLE, 3);
    }

    /**
     * Creates a new optimistic transaction with the specified isolation level.
     *
     * @param tablePath      the path to the Delta table
     * @param isolationLevel the isolation level to use
     * @param maxRetryCount  the maximum number of times to retry on conflict
     */
    public OptimisticTransaction(Storage storage, String tablePath, IsolationLevel isolationLevel, int maxRetryCount) throws IOException {
        super(storage, tablePath);
        this.storage = storage;
        this.isolationLevel = isolationLevel;
        this.newMetadata = new HashMap<>();
        this.readPredicates = new HashSet<>();
        this.retryCount = new AtomicInteger(0);
        this.maxRetryCount = maxRetryCount;
        this.deltaLog = new DeltaLog(storage, tablePath);
        this.snapshot = deltaLog.snapshot();
        this.readVersion = snapshot.getVersion();

    }

    /**
     * Gets the read version of this transaction.
     *
     * @return the read version
     */
    public long getReadVersion() {
        return readVersion;
    }

    /**
     * Gets the DeltaLog associated with this transaction.
     *
     * @return the DeltaLog
     */
    public DeltaLog getDeltaLog() {
        return deltaLog;
    }

    /**
     * Records that a predicate was read as part of this transaction.
     *
     * @param predicate the predicate that was read
     * @return this transaction for chaining
     */
    public OptimisticTransaction readPredicate(String predicate) {
        readPredicates.add(predicate);
        return this;
    }

    /**
     * Records file read operations for conflict detection.
     *
     * @param path the path of the file that was read
     * @return this transaction for chaining
     */
    public OptimisticTransaction readFile(String path) {
        return readPredicate("file:" + path);
    }

    /**
     * Records metadata read operations for conflict detection.
     *
     * @param key the metadata key that was read
     * @return this transaction for chaining
     */
    public OptimisticTransaction readMetadata(String key) {
        return readPredicate("metadata:" + key);
    }

    /**
     * Updates metadata as part of this transaction.
     *
     * @param key   the metadata key to update
     * @param value the new value
     * @return this transaction for chaining
     */
    public OptimisticTransaction updateMetadata(String key, String value) {
        Map<String, String> metadataMap = newMetadata.computeIfAbsent(key, k -> new HashMap<>());
        metadataMap.put(key, value);
        return this;
    }

    public List<Action> insert(List<Map<String, String>> records) throws IOException {
        List<Action> actions = new DeltaTable(storage, tablePath.toString()).insert(records);
        actions.stream().forEach(action -> {
            addAction(action);
        });
        return actions;
    }


    /**
     * Reads all records from the Delta table.
     *
     * @return a list of records, where each record is a map of column names to values
     * @throws IOException if an I/O error occurs
     */
    public List<Map<String, String>> readAll() throws IOException {
        List<Map<String, String>> allRecords = new ArrayList<>();

        // Get all active files at a snapshot
        List<AddFile> activeFiles = snapshot.getAllFiles();

        // Read each file and collect records
        for (AddFile addFile : activeFiles) {
            Path filePath = Paths.get(tablePath.toString(), addFile.getPath());
            List<Map<String, String>> fileRecords = ParquetUtil.readRecords(filePath);
            allRecords.addAll(fileRecords);
        }

        return allRecords;
    }

    @Override
    public OptimisticTransaction commit() throws IOException {
        return commit("TRANSACTION");
    }

    /**
     * Commits the transaction with the specified operation name.
     *
     * @param operation the name of the operation
     * @return this transaction for chaining
     * @throws IOException                     if an I/O error occurs
     * @throws ConcurrentModificationException if a conflict is detected
     */
    public OptimisticTransaction commit(String operation) throws IOException {
        //atomic.
        //locks.. //get lock from zookeeper
        try {
            deltaLog.lock();

            // Check for conflicts with concurrent transactions
            checkForConflicts();

            // Add a commit info action
            CommitInfo commitInfo = CommitInfo.create(operation)
                    .withParameter("isolationLevel", isolationLevel.toString())
                    .withParameter("startVersion", String.valueOf(readVersion))
                    .withParameter("commitTime", String.valueOf(Instant.now().toEpochMilli()));

            addAction(commitInfo);

            // Calculate the next version
            long nextVersion = readVersion + 1;

            // Write the actions to the log
            deltaLog.write(nextVersion, getActions());
            deltaLog.update();
            return this;
        } finally {
            deltaLog.releaseLock();//release lock from zookeeper
        }
    }

    /**
     * Checks for conflicts with concurrent transactions.
     *
     * @throws ConcurrentModificationException if a conflict is detected
     * @throws IOException                     if an I/O error occurs
     */
    private void checkForConflicts() throws IOException {
        // Force update the DeltaLog to get the latest state
        Snapshot currentSnapshot = deltaLog.update();
        if (currentSnapshot != null && currentSnapshot.getVersion() > readVersion) {
            throw new ConcurrentModificationException("\"Conflict detected: File added that affects a predicate read by this transaction");
        }
    }

    /**
     * Reads actions that were committed between the specified versions.
     *
     * @param startVersion the start version (inclusive)
     * @param endVersion   the end version (inclusive)
     * @return list of actions in those versions
     * @throws IOException if an I/O error occurs
     */
    private List<Action> readConcurrentActions(long startVersion, long endVersion) throws IOException {
        List<Action> actions = new ArrayList<>();

        for (long version = startVersion; version <= endVersion; version++) {
            actions.addAll(deltaLog.readVersion(version));
        }

        return actions;
    }

    /**
     * Commits the transaction with automatic retry on conflicts.
     *
     * @param operation the name of the operation
     * @return the version that was committed
     * @throws IOException if an I/O error occurs after max retries
     */
    public long commitWithRetry(String operation) throws IOException {
        int attemptCount = 0;

        while (true) {
            try {
                commit(operation);
                return deltaLog.getLatestVersion();
            } catch (ConcurrentModificationException e) {
                attemptCount++;
                if (attemptCount >= maxRetryCount) {
                    throw new IOException("Failed to commit after " + maxRetryCount + " attempts", e);
                }

                // Add some backoff before retrying
                try {
                    Thread.sleep(50 * (long) Math.pow(2, attemptCount));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted during retry", ie);
                }
            }
        }
    }

    /**
     * Executes the transaction operation with automatic retry on conflicts.
     *
     * @param operation the operation to execute
     * @param <T>       the return type of the operation
     * @return the result of the operation
     * @throws IOException if an I/O error occurs
     */
    public static <T> T executeWithRetry(Storage storage, Supplier<T> operation) throws IOException {
        OptimisticTransaction tx = new OptimisticTransaction(storage, "table");
        int attemptCount = 0;

        while (true) {
            try {
                return operation.get();
            } catch (ConcurrentModificationException e) {
                attemptCount++;
                if (attemptCount >= tx.maxRetryCount) {
                    throw new IOException("Failed to commit after " + tx.maxRetryCount + " attempts", e);
                }
                // Add some backoff before retrying
                try {
                    Thread.sleep(50 * (long) Math.pow(2, attemptCount));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted during retry", ie);
                }
            }
        }
    }
} 