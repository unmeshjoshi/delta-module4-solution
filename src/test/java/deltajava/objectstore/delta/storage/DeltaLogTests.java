package deltajava.objectstore.delta.storage;

import deltajava.network.MessageBus;
import deltajava.network.NetworkEndpoint;
import deltajava.objectstore.Client;
import deltajava.objectstore.LocalStorageNode;
import deltajava.objectstore.Server;
import deltajava.objectstore.delta.storage.actions.Action;
import deltajava.objectstore.delta.storage.actions.AddFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DeltaLogTests {
    @TempDir
    Path tempDir;

    private MessageBus messageBus;
    private Client client;
    private Server server;
    private NetworkEndpoint clientEndpoint;
    private NetworkEndpoint serverEndpoint;
    private LocalStorageNode storageNode;
    private ObjectStorage storage;

    @BeforeEach
    void setUp() throws IOException {
        messageBus = new MessageBus();
        clientEndpoint = new NetworkEndpoint("localhost", 9190);
        serverEndpoint = new NetworkEndpoint("localhost", 9191);
        storageNode = new LocalStorageNode(tempDir.toString());
        server = new Server("testServer", storageNode, messageBus, serverEndpoint);
        client = new Client(messageBus, clientEndpoint, Collections.singletonList(serverEndpoint));
        messageBus.registerHandler(serverEndpoint, server);
        messageBus.registerHandler(clientEndpoint, client);
        messageBus.start();
        storage = new ObjectStorage(client);
    }

    @AfterEach
    void tearDown() {
        messageBus.stop();
    }

    @Test
    public void shouldInitialiseDataAndLogPaths() {
        String tablePath = "table1";
        var log = new DeltaLog(storage, tablePath);

        assertEquals("table1/_delta_log/", log.getLogPath());
        assertEquals("table1/data/", log.getDataPath());
    }

    @Test
    public void testGetLatestVersionWithNoLogFiles() throws IOException {
        String tablePath = "emptyTable";
        var log = new DeltaLog(storage, tablePath);
        
        assertEquals(-1, log.getLatestVersion(), "Should return -1 when no log files exist");
    }
    
    @Test
    public void testGetLatestVersionWithMultipleLogFiles() throws IOException {
        String tablePath = "table2";
        var log = new DeltaLog(storage, tablePath);
        String logPath = log.getLogPath();
        
        // Create log files with different versions using DeltaLogFileName
        storage.writeObject(
            LogFileName.fromVersion(0).getPathIn(logPath),
            "{}".getBytes()
        );
        storage.writeObject(
            LogFileName.fromVersion(5).getPathIn(logPath),
            "{}".getBytes()
        );
        storage.writeObject(
            LogFileName.fromVersion(2).getPathIn(logPath),
            "{}".getBytes()
        );
        storage.writeObject(logPath + "README.md", "Delta Log".getBytes()); // Non-version file
        
        assertEquals(5, log.getLatestVersion(), "Should return the highest version number");
    }
    
    @Test
    public void testDeltaLogFileName() {
        // Test creating from version number
        LogFileName versionZero = LogFileName.fromVersion(0);
        assertEquals("00000000000000000000.json", versionZero.getFileName());
        assertEquals(0, versionZero.getVersion());
        
        LogFileName version123 = LogFileName.fromVersion(123);
        assertEquals("00000000000000000123.json", version123.getFileName());
        assertEquals(123, version123.getVersion());
        
        // Test parsing from filename
        LogFileName fromFile = new LogFileName("00000000000000000456.json");
        assertEquals(456, fromFile.getVersion());
        
        // Test parsing from path
        LogFileName fromPath = new LogFileName("/some/path/00000000000000000789.json");
        assertEquals(789, fromPath.getVersion());
        
        // Test path generation
        assertEquals("dir/00000000000000000123.json", version123.getPathIn("dir"));
        assertEquals("dir/00000000000000000123.json", version123.getPathIn("dir/"));
        
        // Test invalid filename
        assertThrows(IllegalArgumentException.class, () -> new LogFileName("invalid.txt"));
        assertThrows(IllegalArgumentException.class, () -> new LogFileName("abc123.json"));
        assertThrows(IllegalArgumentException.class, () -> LogFileName.fromVersion(-1));
    }

    @Test
    public void testExtractVersionFromFileName() {
        // Test valid filenames
        assertEquals(0, LogFileName.versionFromName("00000000000000000000.json"));
        assertEquals(42, LogFileName.versionFromName("/path/to/00000000000000000042.json"));
        assertEquals(999, LogFileName.versionFromName("00000000000000000999.json"));
        
        // Test invalid filenames
        assertEquals(-1, LogFileName.versionFromName("invalid.txt"));
        assertEquals(-1, LogFileName.versionFromName("abc123.json"));
        assertEquals(-1, LogFileName.versionFromName("README.md"));
        assertEquals(-1, LogFileName.versionFromName(null));
    }
    
    @Test
    public void testListVersionsWithNoLogFiles() {
        String tablePath = "emptyTableVersions";
        var log = new DeltaLog(storage, tablePath);
        
        var versions = log.listVersions();
        assertTrue(versions.isEmpty(), "Should return empty list when no log files exist");
    }
    
    @Test
    public void testListVersionsWithMultipleLogFiles() throws IOException {
        String tablePath = "tableWithVersions";
        var log = new DeltaLog(storage, tablePath);
        String logPath = log.getLogPath();
        
        // Create log files with different versions
        storage.writeObject(
            LogFileName.fromVersion(0).getPathIn(logPath),
            "{}".getBytes()
        );
        storage.writeObject(
            LogFileName.fromVersion(5).getPathIn(logPath),
            "{}".getBytes()
        );
        storage.writeObject(
            LogFileName.fromVersion(2).getPathIn(logPath),
            "{}".getBytes()
        );
        storage.writeObject(logPath + "README.md", "Delta Log".getBytes()); // Non-version file
        
        var versions = log.listVersions();
        
        // Check the results
        assertEquals(3, versions.size(), "Should return exactly 3 valid versions");
        assertTrue(versions.contains(0L), "Should contain version 0");
        assertTrue(versions.contains(2L), "Should contain version 2");
        assertTrue(versions.contains(5L), "Should contain version 5");
    }
    
    @Test
    public void testWriteWithSingleAction() throws IOException {
        // Setup
        String tablePath = "writeTestTable";
        var log = new DeltaLog(storage, tablePath);
        String logPath = log.getLogPath();
        
        // Create a simple add file action
        AddFile addAction = new AddFile("data/file1.parquet", 1024, System.currentTimeMillis());
        List<Action> actions = Collections.singletonList(addAction);
        
        // Execute the write method
        long version = 0;
        log.write(version, actions);
        
        // Verify the log file was written
        String expectedPath = logPath + "00000000000000000000.json";
        byte[] storedData = storage.readObject(expectedPath);
        assertNotNull(storedData, "Log file should exist");
        
        // Verify the content matches what we expect
        String content = new String(storedData);
        assertTrue(content.contains("\"type\":\"add\""), "Should contain action type");
        assertTrue(content.contains("\"path\":\"data/file1.parquet\""), "Should contain file path");
        assertTrue(content.contains("\"size\":1024"), "Should contain file size");
    }
    
    @Test
    public void testWriteWithMultipleActions() throws IOException {
        // Setup
        String tablePath = "writeTestMultiple";
        var log = new DeltaLog(storage, tablePath);
        String logPath = log.getLogPath();
        
        // Create multiple actions
        long currentTime = System.currentTimeMillis();
        List<Action> actions = new ArrayList<>();
        
        // Add file action
        AddFile addAction1 = new AddFile("data/file1.parquet", 1024, currentTime);
        actions.add(addAction1);
        
        // Another add file action
        Map<String, String> partitionValues = new HashMap<>();
        partitionValues.put("date", "2023-04-01");
        AddFile addAction2 = new AddFile("data/date=2023-04-01/file2.parquet", 
                                        partitionValues, 2048, currentTime, 
                                        true, new HashMap<>(), "");
        actions.add(addAction2);
        
        // Execute the write method with version 5
        long version = 5;
        log.write(version, actions);
        
        // Verify the log file was written
        String expectedPath = logPath + "00000000000000000005.json";
        byte[] storedData = storage.readObject(expectedPath);
        assertNotNull(storedData, "Log file should exist");
        
        // Verify the content contains both actions
        String content = new String(storedData);
        
        // Should have two 'add' types
        int addTypeCount = content.split("\"type\":\"add\"").length - 1;
        assertEquals(2, addTypeCount, "Should contain two add actions");
        
        // Verify specific content for both files
        assertTrue(content.contains("\"path\":\"data/file1.parquet\""), "Should contain first file path");
        assertTrue(content.contains("\"path\":\"data/date=2023-04-01/file2.parquet\""), "Should contain second file path");
        assertTrue(content.contains("\"date\":\"2023-04-01\""), "Should contain partition values");
    }
    
    @Test
    public void testWriteAndListVersions() throws IOException {
        // Setup
        String tablePath = "writeAndListTable";
        var log = new DeltaLog(storage, tablePath);
        
        // Create two versions with different actions
        AddFile addAction1 = new AddFile("data/file1.parquet", 1024, System.currentTimeMillis());
        log.write(0, Collections.singletonList(addAction1));
        
        AddFile addAction2 = new AddFile("data/file2.parquet", 2048, System.currentTimeMillis());
        log.write(5, Collections.singletonList(addAction2));
        
        // List the versions
        List<Long> versions = log.listVersions();
        
        // Verify both versions are listed
        assertEquals(2, versions.size(), "Should list two versions");
        assertTrue(versions.contains(0L), "Should contain version 0");
        assertTrue(versions.contains(5L), "Should contain version 5");
        
        // Verify the latest version is 5
        assertEquals(5, log.getLatestVersion(), "Latest version should be 5");
    }
    
    @Test
    public void testWriteWithInvalidVersion() {
        // Setup
        String tablePath = "invalidVersionTable";
        var log = new DeltaLog(storage, tablePath);
        
        // Try to write with negative version
        AddFile addAction = new AddFile("data/file1.parquet", 1024, System.currentTimeMillis());
        List<Action> actions = Collections.singletonList(addAction);
        
        // Expect an exception for negative version
        assertThrows(IllegalArgumentException.class, () -> {
            log.write(-1, actions);
        }, "Should throw exception for negative version");
    }

    @Test
    public void testSnapshotWithNoVersions() throws IOException {
        // Setup
        String tablePath = "emptySnapshotTable";
        var log = new DeltaLog(storage, tablePath);
        
        // Get snapshot of empty table
        Snapshot snapshot = log.snapshot();
        
        // Verify
        assertEquals(-1, snapshot.getVersion(), "Empty table should have version -1");
        assertTrue(snapshot.getActions().isEmpty(), "Empty table should have no actions");
        assertTrue(snapshot.getAllFiles().isEmpty(), "Empty table should have no files");
    }
    
    @Test
    public void testSnapshotWithSingleVersion() throws IOException {
        // Setup
        String tablePath = "singleVersionTable";
        var log = new DeltaLog(storage, tablePath);
        
        // Create a version
        AddFile addAction = new AddFile("data/file1.parquet", 1024, System.currentTimeMillis());
        log.write(0, Collections.singletonList(addAction));
        
        // Get snapshot
        Snapshot snapshot = log.snapshot();
        
        // Verify
        assertEquals(0, snapshot.getVersion(), "Snapshot should have version 0");
        assertEquals(1, snapshot.getActions().size(), "Snapshot should have one action");
        assertEquals(1, snapshot.getAllFiles().size(), "Snapshot should have one file");
        assertEquals("data/file1.parquet", snapshot.getAllFiles().get(0).getPath(), 
                     "File path should match");
    }
    
    @Test
    public void testSnapshotWithMultipleVersions() throws IOException {
        // Setup
        String tablePath = "multiVersionTable";
        var log = new DeltaLog(storage, tablePath);
        
        // Create version 0
        AddFile addAction1 = new AddFile("data/file1.parquet", 1024, System.currentTimeMillis());
        log.write(0, Collections.singletonList(addAction1));
        
        // Create version 1
        AddFile addAction2 = new AddFile("data/file2.parquet", 2048, System.currentTimeMillis());
        log.write(1, Collections.singletonList(addAction2));
        
        // Get snapshot - should reflect all versions combined
        Snapshot snapshot = log.snapshot();
        
        // Verify
        assertEquals(1, snapshot.getVersion(), "Snapshot should have latest version (1)");
        assertEquals(2, snapshot.getActions().size(), "Snapshot should have actions from all versions");
        assertEquals(2, snapshot.getAllFiles().size(), "Snapshot should have files from all versions");
        
        // Verify contents from both versions are present
        List<String> filePaths = snapshot.getAllFiles().stream()
            .map(AddFile::getPath)
            .collect(java.util.stream.Collectors.toList());
        
        assertTrue(filePaths.contains("data/file1.parquet"), "Should contain file from version 0");
        assertTrue(filePaths.contains("data/file2.parquet"), "Should contain file from version 1");
    }
    
    @Test
    public void testUpdate() throws IOException {
        // Setup
        String tablePath = "updateTable";
        var log = new DeltaLog(storage, tablePath);
        
        // Create a version
        AddFile addAction = new AddFile("data/file1.parquet", 1024, System.currentTimeMillis());
        log.write(0, Collections.singletonList(addAction));
        
        // Get updated snapshot
        Snapshot snapshot1 = log.update();
        
        // Verify
        assertEquals(0, snapshot1.getVersion(), "Updated snapshot should have version 0");
        assertEquals(1, snapshot1.getActions().size(), "Updated snapshot should have one action");
        assertEquals(1, snapshot1.getAllFiles().size(), "Updated snapshot should have one file");
        
        // Call update again without changing the version
        Snapshot snapshot2 = log.update();
        
        // Should be the same object instance (cached)
        assertSame(snapshot1, snapshot2, "Should return cached snapshot when version hasn't changed");
        
        // Create another version
        AddFile addAction2 = new AddFile("data/file2.parquet", 2048, System.currentTimeMillis());
        log.write(1, Collections.singletonList(addAction2));
        
        // Update again - should get a new snapshot
        Snapshot snapshot3 = log.update();
        
        // Should be a different object with updated version
        assertNotSame(snapshot2, snapshot3, "Should create new snapshot when version has changed");
        assertEquals(1, snapshot3.getVersion(), "New snapshot should have updated version");
        assertEquals(2, snapshot3.getActions().size(), "New snapshot should include actions from all versions");
    }
    
    @Test
    public void testLocking() throws IOException {
        // Setup
        String tablePath = "lockingTable";
        var log = new DeltaLog(storage, tablePath);
        
        // Test acquiring and releasing lock
        log.lock();
        
        // Write while holding the lock
        AddFile addAction = new AddFile("data/locked-file.parquet", 1024, System.currentTimeMillis());
        log.write(0, Collections.singletonList(addAction));
        
        // Release the lock
        log.releaseLock();
        
        // Verify we can still read what we wrote
        Snapshot snapshot = log.snapshot();
        assertEquals(0, snapshot.getVersion(), "Should read version written under lock");
        assertEquals(1, snapshot.getAllFiles().size(), "Should have the file we wrote under lock");
    }
}