package deltajava.objectstore;

import deltajava.network.NetworkEndpoint;
import deltajava.objectstore.ObjectStoreCluster.ServerNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ObjectStorePartitioningTest {
    private Client client;
    private NetworkEndpoint clientEndpoint;
    private ObjectStoreCluster cluster;

    @BeforeEach
    void setUp(@TempDir Path tempDir) {
        int numServers = 10;
        cluster = new ObjectStoreCluster(tempDir, numServers);
        cluster.start();
        client = cluster.getClient();
    }

    @Test
    void testSingleObjectInsertion() throws Exception {
        // Create a single customer profile
        CustomerProfile customer = new CustomerProfile(
            "CUST0001",
            "John Doe",
            "john@example.com",
            750
        );

        // Insert the customer data
        String key = "customer-" + customer.getId();
        String jsonData = customer.toJson();
        client.putObject(key, jsonData.getBytes()).get(5, TimeUnit.SECONDS);

        // Verify the data was inserted correctly
        byte[] retrievedData = client.getObject(key).get(5, TimeUnit.SECONDS);
        String retrievedJson = new String(retrievedData);
        CustomerProfile retrievedCustomer = CustomerProfile.fromJson(retrievedJson);
        
        assertEquals(customer, retrievedCustomer, "Retrieved customer should match inserted customer");

        // Verify data distribution
        System.out.println("\nData Distribution After Single Insert:");
        for (ServerNode node : cluster.getServerNodes()) {
            int count = countObjectsForServer(node.storage);
            System.out.printf("Server %s (%s): %d objects%n", 
                node.id, 
                node.endpoint.toString(), 
                count);
        }
    }

    @Test
    void testBatchObjectInsertion() throws Exception {
        // Create multiple customer profiles
        List<CustomerProfile> customers = createCustomerProfiles(5);
        
        // Insert all customers
        for (CustomerProfile customer : customers) {
            String key = "customer-" + customer.getId();
            String jsonData = customer.toJson();
            client.putObject(key, jsonData.getBytes()).get(5, TimeUnit.SECONDS);
        }

        // Verify all customers were inserted correctly
        for (CustomerProfile customer : customers) {
            String key = "customer-" + customer.getId();
            byte[] retrievedData = client.getObject(key).get(5, TimeUnit.SECONDS);
            String retrievedJson = new String(retrievedData);
            CustomerProfile retrievedCustomer = CustomerProfile.fromJson(retrievedJson);
            assertEquals(customer, retrievedCustomer, "Retrieved customer should match inserted customer");
        }

        // Verify data distribution
        System.out.println("\nData Distribution After Batch Insert:");
        for (ServerNode node : cluster.getServerNodes()) {
            int count = countObjectsForServer(node.storage);
            System.out.printf("Server %s (%s): %d objects%n", 
                node.id, 
                node.endpoint.toString(), 
                count);
        }
    }

    @Test
    void testConcurrentObjectInsertion() throws Exception {
        // Create multiple customer profiles
        List<CustomerProfile> customers = createCustomerProfiles(10);
        
        // Insert all customers concurrently
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (CustomerProfile customer : customers) {
            String key = "customer-" + customer.getId();
            String jsonData = customer.toJson();
            futures.add(client.putObject(key, jsonData.getBytes()));
        }

        // Wait for all insertions to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .get(10, TimeUnit.SECONDS);

        // Verify all customers were inserted correctly
        for (CustomerProfile customer : customers) {
            String key = "customer-" + customer.getId();
            byte[] retrievedData = client.getObject(key).get(5, TimeUnit.SECONDS);
            String retrievedJson = new String(retrievedData);
            CustomerProfile retrievedCustomer = CustomerProfile.fromJson(retrievedJson);
            assertEquals(customer, retrievedCustomer, "Retrieved customer should match inserted customer");
        }

        // Verify data distribution
        System.out.println("\nData Distribution After Concurrent Insert:");
        for (ServerNode node : cluster.getServerNodes()) {
            int count = countObjectsForServer(node.storage);
            System.out.printf("Server %s (%s): %d objects%n", 
                node.id, 
                node.endpoint.toString(), 
                count);
        }
    }


    private List<CustomerProfile> createCustomerProfiles(int count) {
        List<CustomerProfile> customers = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            customers.add(new CustomerProfile(
                "CUST" + String.format("%04d", i),
                "Customer " + i,
                "customer" + i + "@example.com",
                new Random().nextInt(1000000)
            ));
        }
        return customers;
    }

    private int countObjectsForServer(LocalStorageNode storageNode) {
        try {
            return storageNode.listObjects("").size();
        } catch (Exception e) {
            return 0;
        }
    }


    private static class CustomerProfile implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String id;
        private final String name;
        private final String email;
        private final int creditScore;

        public CustomerProfile(String id, String name, String email, int creditScore) {
            this.id = id;
            this.name = name;
            this.email = email;
            this.creditScore = creditScore;
        }

        public String getId() { return id; }
        public String getName() { return name; }
        public String getEmail() { return email; }
        public int getCreditScore() { return creditScore; }

        public String toJson() {
            return String.format(
                "{\"id\":\"%s\",\"name\":\"%s\",\"email\":\"%s\",\"creditScore\":%d}",
                id, name, email, creditScore
            );
        }

        public static CustomerProfile fromJson(String json) {
            // Simple JSON parsing - in a real implementation, use a proper JSON library
            String[] parts = json.substring(1, json.length() - 1).split(",");
            String id = parts[0].split(":")[1].replace("\"", "");
            String name = parts[1].split(":")[1].replace("\"", "");
            String email = parts[2].split(":")[1].replace("\"", "");
            int creditScore = Integer.parseInt(parts[3].split(":")[1]);
            return new CustomerProfile(id, name, email, creditScore);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CustomerProfile that = (CustomerProfile) o;
            return creditScore == that.creditScore &&
                   Objects.equals(id, that.id) &&
                   Objects.equals(name, that.name) &&
                   Objects.equals(email, that.email);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name, email, creditScore);
        }
    }
} 