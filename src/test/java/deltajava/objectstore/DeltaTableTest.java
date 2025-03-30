package deltajava.objectstore;

import deltajava.objectstore.delta.storage.DeltaTable;
import deltajava.objectstore.delta.storage.ObjectStorage;
import deltajava.objectstore.delta.storage.Storage;
import deltajava.objectstore.delta.storage.actions.Action;
import deltajava.objectstore.delta.storage.actions.AddFile;
import deltajava.objectstore.delta.storage.util.ParquetUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static deltajava.objectstore.TestDataGenerator.createSampleCustomers;
import static org.junit.jupiter.api.Assertions.*;

class DeltaTableTest {
    private static final String TABLE_PATH = "customer_table";
    private static final int TEST_TIMEOUT_SECONDS = 5;
    private static final int SERVER_COUNT = 3;
    
    @TempDir
    Path tempDir;
    
    private ObjectStoreCluster cluster;
    private Client client;
    private Storage storage;
    private DeltaTable deltaTable;
    
    @BeforeEach
    void setUp() throws IOException {
        // Setup cluster using the ObjectStoreCluster helper
        cluster = new ObjectStoreCluster(tempDir, SERVER_COUNT);
        cluster.start(); // This handles registration of handlers to message bus
        
        // Get client from the cluster
        client = cluster.getClient();
        
        // Setup delta table with storage
        storage = new ObjectStorage(client);
        deltaTable = new DeltaTable(storage, TABLE_PATH);
    }
    
    @AfterEach
    void tearDown() {
        cluster.stop();
    }
    
    @Test
    void shouldInsertAndReadCustomersUsingDeltaTable() throws IOException {
        // Create sample customer data using the TestDataGenerator
        List<Map<String, String>> customers = createSampleCustomers(10);
        
        // Insert customers using DeltaTable
        List<Action> actions = deltaTable.insert(customers);
        
        // Verify actions were created
        assertFalse(actions.isEmpty(), "Should return at least one action");
        assertTrue(actions.get(0) instanceof AddFile, "Should return AddFile action");
        
        // Get the path of the created file
        AddFile addFile = (AddFile) actions.get(0);
        String filePath = TABLE_PATH + "/" + addFile.getPath();
        
        // Verify file was actually written to storage
        verifyFileWrittenToStorage(filePath, customers);
        
        // Additional verification: Check data distribution across servers
        verifyDataDistribution();
    }
    
    /**
     * Verifies that data is properly distributed across the cluster.
     */
    private void verifyDataDistribution() {
        // At least one server should have data
        boolean anyServerHasData = cluster.getServerNodes().stream()
            .map(node -> cluster.countObjectsForServer(node.storage))
            .anyMatch(count -> count > 0);
        
        assertTrue(anyServerHasData, "Data should be distributed to at least one server");
    }
    
    /**
     * Verifies that a file was properly written to storage and contains the expected records.
     */
    private void verifyFileWrittenToStorage(String filePath, List<Map<String, String>> expectedRecords) 
            throws IOException {
        try {
            // Read file from storage
            byte[] fileData = client.getObject(filePath).get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertNotNull(fileData, "File data should exist");
            assertTrue(fileData.length > 0, "File data should not be empty");
            
            // Write to temporary file for Parquet reading
            Path tempFilePath = Files.createTempFile("test-verify-", ".parquet");
            try {
                Files.write(tempFilePath, fileData);
                
                // Read records from Parquet
                List<Map<String, String>> readCustomers = ParquetUtil.readRecords(tempFilePath);
                
                // Verify the correct number of customers were read
                assertEquals(expectedRecords.size(), readCustomers.size(), 
                        "Should read the same number of customers that were inserted");
                
                // Verify customer data was preserved correctly
                for (int i = 0; i < expectedRecords.size(); i++) {
                    Map<String, String> original = expectedRecords.get(i);
                    Map<String, String> read = readCustomers.get(i);
                    
                    assertEquals(original.get("id"), read.get("id"), "Customer ID should match");
                    assertEquals(original.get("name"), read.get("name"), "Customer name should match");
                    assertEquals(original.get("email"), read.get("email"), "Customer email should match");
                    assertEquals(original.get("age"), read.get("age"), "Customer age should match");
                    assertEquals(original.get("city"), read.get("city"), "Customer city should match");
                }
            } finally {
                Files.deleteIfExists(tempFilePath);
            }
        } catch (Exception e) {
            fail("Failed to retrieve or process file from storage: " + e.getMessage());
        }
    }
} 