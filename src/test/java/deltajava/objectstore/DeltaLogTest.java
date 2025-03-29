package deltajava.objectstore;

import deltajava.network.MessageBus;
import deltajava.network.NetworkEndpoint;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import static org.junit.jupiter.api.Assertions.*;

class DeltaLogTest {
    @TempDir
    java.nio.file.Path tempDir;
    
    private List<LocalStorageNode> storageNodes;
    private MessageBus messageBus;
    private List<Server> servers;
    private Client client;
    private List<NetworkEndpoint> serverEndpoints;
    private NetworkEndpoint clientEndpoint;
    private ObjectMapper objectMapper;
    
    @BeforeEach
    void setUp() throws IOException {
        setupStorageNodes();
        setupMessageBus();
        setupServers();
        setupClient();
        startMessageBus();
        objectMapper = new ObjectMapper();
    }
    
    private void setupStorageNodes() throws IOException {
        storageNodes = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            java.nio.file.Path storagePath = tempDir.resolve("storage" + i);
            storageNodes.add(new LocalStorageNode(storagePath.toString()));
        }
    }
    
    private void setupMessageBus() {
        messageBus = new MessageBus();
        serverEndpoints = Arrays.asList(
            new NetworkEndpoint("localhost", 8081),
            new NetworkEndpoint("localhost", 8082),
            new NetworkEndpoint("localhost", 8083)
        );
        clientEndpoint = new NetworkEndpoint("localhost", 8080);
    }
    
    private void setupServers() {
        servers = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Server server = new Server("server" + i, storageNodes.get(i), messageBus, serverEndpoints.get(i));
            servers.add(server);
            messageBus.registerHandler(serverEndpoints.get(i), server);
        }
    }
    
    private void setupClient() {
        client = new Client(messageBus, clientEndpoint, serverEndpoints);
        messageBus.registerHandler(clientEndpoint, client);
    }
    
    private void startMessageBus() {
        messageBus.start();
    }
    
    @Test
    void shouldStoreAndRetrieveJsonLogsInDeltaTable() throws Exception {
        // Given: We have multiple transaction log entries
        List<String> logFiles = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            ObjectNode transactionLog = createTransactionLog(i);
            String logPath = createLogPath(i);
            logFiles.add(logPath);
            
            // When: We store each log entry in the object store
            byte[] logData = objectMapper.writeValueAsBytes(transactionLog);
            client.putObject(logPath, logData).join();
            
            // Then: We can read back what we wrote
            byte[] storedData = client.getObject(logPath).join();
            assertNotNull(storedData);
            assertEquals(
                objectMapper.readTree(logData),
                objectMapper.readTree(storedData),
                "Stored JSON should match the original"
            );
        }
        
        // When: We attempt to read all logs in order
        // Then: All logs exist and have the correct structure
        for (String logPath : logFiles) {
            byte[] data = client.getObject(logPath).join();
            assertNotNull(data, "Log file should exist: " + logPath);
            
            ObjectNode logEntry = (ObjectNode) objectMapper.readTree(data);
            assertTrue(logEntry.has("version"), "Log should have version");
            assertTrue(logEntry.has("timestamp"), "Log should have timestamp");
            assertTrue(logEntry.has("operation"), "Log should have operation");
        }
    }
    
    private ObjectNode createTransactionLog(int version) throws IOException {
        String jsonTemplate = """
            {
              "version": %d,
              "timestamp": %d,
              "operation": {
                "op": "ADD",
                "path": "/data/file_%d.parquet",
                "partitionValues": {"date": "2024-03-20"}
              }
            }
            """;
        
        long timestamp = System.currentTimeMillis();
        String json = String.format(jsonTemplate, version, timestamp, version);
        
        return (ObjectNode) objectMapper.readTree(json);
    }
    
    private String createLogPath(int version) {
        return String.format("_delta_log/%020d.json", version);
    }
} 