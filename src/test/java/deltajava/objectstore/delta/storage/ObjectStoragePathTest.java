package deltajava.objectstore.delta.storage;

import deltajava.network.MessageBus;
import deltajava.network.NetworkEndpoint;
import deltajava.objectstore.Client;
import deltajava.objectstore.LocalStorageNode;
import deltajava.objectstore.ObjectStoreCluster;
import deltajava.objectstore.Server;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ObjectStoragePathTest {
    @TempDir
    Path tempDir;
    
    private ObjectStoreCluster cluster;

    private ObjectStorage storage;
    
    @BeforeEach
    void setUp() throws IOException {
        cluster = new ObjectStoreCluster(tempDir, 3);
        cluster.start();
        storage = new ObjectStorage(cluster.getClient());
    }
    
    @AfterEach
    void tearDown() {
        cluster.stop();
    }
    
    @Test
    void shouldHandleNestedPaths() throws IOException {
        // Given
        String basePath = "table1";
        String logPath = basePath + "/_delta_log/00000000000000000000.json";
        String content = "{\"version\": 0}";
        byte[] data = content.getBytes(StandardCharsets.UTF_8);
        
        // When
        storage.writeObject(logPath, data);
        
        // Then
        assertTrue(storage.objectExists(logPath));
        List<String> objects = storage.listObjects(basePath + "/_delta_log");
        assertEquals(1, objects.size());
        assertTrue(objects.contains(logPath));
    }
    
    @Test
    void shouldHandlePathsWithSpecialCharacters() throws IOException {
        // Given
        String pathWithSpaces = "table with spaces/file.txt";
        String pathWithSymbols = "table-with-symbols/file@#$.txt";
        String content = "Test content";
        byte[] data = content.getBytes(StandardCharsets.UTF_8);
        
        // When
        storage.writeObject(pathWithSpaces, data);
        storage.writeObject(pathWithSymbols, data);
        
        // Then
        assertTrue(storage.objectExists(pathWithSpaces));
        assertTrue(storage.objectExists(pathWithSymbols));
        
        byte[] spacesData = storage.readObject(pathWithSpaces);
        byte[] symbolsData = storage.readObject(pathWithSymbols);
        
        assertEquals(content, new String(spacesData, StandardCharsets.UTF_8));
        assertEquals(content, new String(symbolsData, StandardCharsets.UTF_8));
    }
    
    @Test
    void shouldSupportHierarchicalListingWithPrefixes() throws IOException {
        // Given
        String tablePath = "db/table/";
        
        // Create a hierarchical structure
        storage.writeObject(tablePath + "_delta_log/00000000000000000000.json", "{\"version\": 0}".getBytes());
        storage.writeObject(tablePath + "_delta_log/00000000000000000001.json", "{\"version\": 1}".getBytes());
        storage.writeObject(tablePath + "data/part-00000.parquet", "data0".getBytes());
        storage.writeObject(tablePath + "data/part-00001.parquet", "data1".getBytes());
        
        // When
        List<String> allObjects = storage.listObjects("db/table");
        List<String> logObjects = storage.listObjects(tablePath + "_delta_log");
        List<String> dataObjects = storage.listObjects(tablePath + "data");
        
        // Then
        assertEquals(4, allObjects.size());
        assertEquals(2, logObjects.size());
        assertEquals(2, dataObjects.size());
        
        assertTrue(logObjects.contains(tablePath + "_delta_log/00000000000000000000.json"));
        assertTrue(logObjects.contains(tablePath + "_delta_log/00000000000000000001.json"));
        assertTrue(dataObjects.contains(tablePath + "data/part-00000.parquet"));
        assertTrue(dataObjects.contains(tablePath + "data/part-00001.parquet"));
    }
    
    @Test
    void shouldMaintainPathConsistencyInOperations() throws IOException {
        // Given
        String path = "db/schema/table/_delta_log/00000000000000000000.json";
        String content = "{\"version\": 0}";
        byte[] data = content.getBytes(StandardCharsets.UTF_8);
        
        // When - Write, read, check exists, list, delete all use the same path
        storage.writeObject(path, data);
        byte[] readData = storage.readObject(path);
        boolean exists = storage.objectExists(path);
        List<String> objects = storage.listObjects("db/schema/table/_delta_log");
        storage.deleteObject(path);
        boolean existsAfterDelete = storage.objectExists(path);
        
        // Then
        assertEquals(content, new String(readData, StandardCharsets.UTF_8));
        assertTrue(exists);
        assertEquals(1, objects.size());
        assertTrue(objects.contains(path));
        assertFalse(existsAfterDelete);
    }
} 