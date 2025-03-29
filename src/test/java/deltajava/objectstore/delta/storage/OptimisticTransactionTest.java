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
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class OptimisticTransactionTest {
    @TempDir
    Path tempDir;

    private MessageBus messageBus;
    private Client client;
    private Server server;
    private NetworkEndpoint clientEndpoint;
    private NetworkEndpoint serverEndpoint;
    private LocalStorageNode storageNode;
    private ObjectStorage storage;
    private String customersTablePath;

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
        customersTablePath = tempDir.resolve("customers_table").toString();
    }

    @AfterEach
    void tearDown() {
        messageBus.stop();
    }
    
    private List<CustomerProfile> createSampleCustomers() {
        List<CustomerProfile> customers = new ArrayList<>();
        customers.add(new CustomerProfile(
            "C001", "John Doe", "john.doe@example.com", 35, 
            "North", 720.5, true, "2023-01-15"
        ));
        customers.add(new CustomerProfile(
            "C002", "Alice Smith", "alice.smith@example.com", 28, 
            "South", 680.0, false, "2023-02-20"
        ));
        customers.add(new CustomerProfile(
            "C003", "Bob Johnson", "bob.johnson@example.com", 42, 
            "East", 750.3, true, "2023-01-05"
        ));
        customers.add(new CustomerProfile(
            "C004", "Emma Wilson", "emma.wilson@example.com", 31, 
            "West", 690.8, false, "2023-03-10"
        ));
        customers.add(new CustomerProfile(
            "C005", "Michael Brown", "michael.brown@example.com", 39, 
            "North", 710.2, true, "2023-02-28"
        ));
        return customers;
    }
    
    private List<Map<String, String>> customerProfilesToMaps(List<CustomerProfile> customers) {
        return customers.stream()
                .map(CustomerProfile::toMap)
                .collect(Collectors.toList());
    }

    @Test
    void testInsertCustomerProfiles() throws IOException {
        // Create sample customers
        List<CustomerProfile> customers = createSampleCustomers();
        List<Map<String, String>> customerMaps = customerProfilesToMaps(customers);
        
        // Create transaction and insert customers
        OptimisticTransaction transaction = new OptimisticTransaction(storage, customersTablePath);
        List<Action> actions = transaction.insert(customerMaps);
        transaction.commit();
        
        // Verify actions were created
        assertEquals(1, actions.size(), "Should create 1 action for the insert");
        assertTrue(actions.get(0) instanceof AddFile, "Action should be an AddFile");
        
        // Read back the data
        OptimisticTransaction readTransaction = new OptimisticTransaction(storage, customersTablePath);
        List<Map<String, String>> readRecords = readTransaction.readAll();
        
        // Verify the number of records
        assertEquals(customers.size(), readRecords.size(), "Should have the same number of records");
        
        // Convert back to CustomerProfile objects for easier comparison
        List<CustomerProfile> readProfiles = readRecords.stream()
                .map(CustomerProfile::fromMap)
                .collect(Collectors.toList());
        
        // Verify some customers exist
        assertTrue(readProfiles.stream().anyMatch(p -> "C001".equals(p.getId())), "Should contain customer C001");
        assertTrue(readProfiles.stream().anyMatch(p -> "C003".equals(p.getId())), "Should contain customer C003");
        assertTrue(readProfiles.stream().anyMatch(p -> "C005".equals(p.getId())), "Should contain customer C005");
        
        // Check some specific values
        CustomerProfile johnDoe = readProfiles.stream()
                .filter(p -> "C001".equals(p.getId()))
                .findFirst()
                .orElse(null);
        
        assertNotNull(johnDoe, "Should find John Doe");
        assertEquals("John Doe", johnDoe.getName(), "Name should match");
        assertEquals("North", johnDoe.getRegion(), "Region should match");
        assertEquals(35, johnDoe.getAge(), "Age should match");
        assertTrue(johnDoe.isPremium(), "Premium status should match");
    }
    
    @Test
    void testSerialInsertWithMultipleTransactions() throws IOException {
        // Create sample customers divided into two groups
        List<CustomerProfile> allCustomers = createSampleCustomers();
        List<CustomerProfile> firstBatch = allCustomers.subList(0, 2);
        List<CustomerProfile> secondBatch = allCustomers.subList(2, 5);
        
        // First transaction
        OptimisticTransaction transaction1 = new OptimisticTransaction(storage, customersTablePath);
        transaction1.insert(customerProfilesToMaps(firstBatch));
        transaction1.commit();
        
        // Verify the version
        DeltaLog log = new DeltaLog(storage, customersTablePath);
        assertEquals(0, log.getLatestVersion(), "First transaction should create version 0");
        
        // Second transaction
        OptimisticTransaction transaction2 = new OptimisticTransaction(storage, customersTablePath);
        transaction2.insert(customerProfilesToMaps(secondBatch));
        transaction2.commit();
        
        // Verify the version increased
        assertEquals(1, log.getLatestVersion(), "Second transaction should create version 1");
        
        // Read back all data
        OptimisticTransaction readTransaction = new OptimisticTransaction(storage, customersTablePath);
        List<Map<String, String>> readRecords = readTransaction.readAll();
        
        // We should have all customers
        assertEquals(allCustomers.size(), readRecords.size(), "Should have all customers");
    }
    
    @Test
    void testConcurrentTransactionsWithConflict() throws IOException {
        // Start with some initial data
        List<CustomerProfile> initialCustomers = createSampleCustomers().subList(0, 2);
        OptimisticTransaction initialTx = new OptimisticTransaction(storage, customersTablePath);
        initialTx.insert(customerProfilesToMaps(initialCustomers));
        initialTx.commit();
        
        // Create two transactions based on the same initial state
        OptimisticTransaction tx1 = new OptimisticTransaction(storage, customersTablePath);
        OptimisticTransaction tx2 = new OptimisticTransaction(storage, customersTablePath);
        
        // Tx1 adds a customer
        CustomerProfile newCustomer1 = new CustomerProfile(
            "C006", "Sarah Lee", "sarah.lee@example.com", 29, 
            "East", 705.6, true, "2023-03-15"
        );
        tx1.insert(Collections.singletonList(newCustomer1.toMap()));
        
        // Commit tx1 first
        tx1.commit();
        
        // Tx2 adds another customer
        CustomerProfile newCustomer2 = new CustomerProfile(
            "C007", "David Wang", "david.wang@example.com", 33, 
            "West", 715.2, false, "2023-03-20"
        );
        tx2.insert(Collections.singletonList(newCustomer2.toMap()));
        
        // Tx2 should fail with an exception because tx1 committed first
        assertThrows(ConcurrentModificationException.class, tx2::commit,
                     "Should throw an exception due to concurrent modification");
    }
}