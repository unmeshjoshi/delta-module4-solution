package deltajava.objectstore.delta.storage;

import deltajava.network.MessageBus;
import deltajava.network.NetworkEndpoint;
import deltajava.objectstore.Client;
import deltajava.objectstore.LocalStorageNode;
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
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ObjectStorageTest {
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
        // Set up the message bus
        messageBus = new MessageBus();
        
        // Set up network endpoints
        clientEndpoint = new NetworkEndpoint("localhost", 9090);
        serverEndpoint = new NetworkEndpoint("localhost", 9091);
        
        // Set up storage node
        storageNode = new LocalStorageNode(tempDir.toString());
        
        // Set up server
        server = new Server("testServer", storageNode, messageBus, serverEndpoint);
        
        // Set up client
        client = new Client(messageBus, clientEndpoint, Arrays.asList(serverEndpoint));
        
        // Register handlers
        messageBus.registerHandler(serverEndpoint, server);
        messageBus.registerHandler(clientEndpoint, client);
        
        // Start message bus
        messageBus.start();
        
        // Create storage implementation
        storage = new ObjectStorage(client);
    }
    
    @AfterEach
    void tearDown() {
        messageBus.stop();
    }
    
    @Test
    void shouldWriteAndReadObject() throws IOException {
        // Given
        String path = "test/data.txt";
        String content = "Hello, Delta Lake!";
        byte[] data = content.getBytes(StandardCharsets.UTF_8);
        
        // When
        storage.writeObject(path, data);
        
        // Then
        byte[] readData = storage.readObject(path);
        assertEquals(content, new String(readData, StandardCharsets.UTF_8));
    }
    
    @Test
    void shouldCheckIfObjectExists() throws IOException {
        // Given
        String path = "test/exists.txt";
        String content = "This file exists";
        byte[] data = content.getBytes(StandardCharsets.UTF_8);
        
        // When
        storage.writeObject(path, data);
        
        // Then
        assertTrue(storage.objectExists(path));
        assertFalse(storage.objectExists("test/doesnotexist.txt"));
    }
    
    @Test
    void shouldDeleteObject() throws IOException {
        // Given
        String path = "test/delete.txt";
        String content = "Delete me";
        byte[] data = content.getBytes(StandardCharsets.UTF_8);
        storage.writeObject(path, data);
        
        // When
        storage.deleteObject(path);
        
        // Then
        assertFalse(storage.objectExists(path));
    }
    
    @Test
    void shouldListObjects() throws IOException {
        // Given
        String prefix = UUID.randomUUID().toString();
        String path1 = prefix + "/file1.txt";
        String path2 = prefix + "/file2.txt";
        String path3 = prefix + "/subfolder/file3.txt";
        String content = "Test content";
        byte[] data = content.getBytes(StandardCharsets.UTF_8);
        
        storage.writeObject(path1, data);
        storage.writeObject(path2, data);
        storage.writeObject(path3, data);
        
        // Also create a file with a different prefix
        storage.writeObject("different/file.txt", data);
        
        // When
        List<String> objects = storage.listObjects(prefix);
        
        // Then
        assertEquals(3, objects.size());
        assertTrue(objects.contains(path1));
        assertTrue(objects.contains(path2));
        assertTrue(objects.contains(path3));
        assertFalse(objects.contains("different/file.txt"));
    }
    
    @Test
    void shouldThrowIOExceptionWhenReadingNonExistentObject() {
        // Given
        String path = "test/nonexistent.txt";
        
        // When/Then
        assertThrows(IOException.class, () -> storage.readObject(path));
    }
} 