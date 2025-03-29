package deltajava.objectstore.delta.storage;

import deltajava.objectstore.Client;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Implementation of Storage that uses the ObjectStore client.
 */
public class ObjectStorage implements Storage {
    private final Client client;
    private final int timeoutSeconds;
    
    public ObjectStorage(Client client) {
        this(client, 10); // Default 10 second timeout
    }
    
    public ObjectStorage(Client client, int timeoutSeconds) {
        this.client = client;
        this.timeoutSeconds = timeoutSeconds;
    }
    
    @Override
    public byte[] readObject(String path) throws IOException {
        try {
            return client.getObject(path).get(timeoutSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new IOException("Failed to read object: " + path, e);
        }
    }
    
    @Override
    public void writeObject(String path, byte[] data) throws IOException {
        try {
            client.putObject(path, data).get(timeoutSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new IOException("Failed to write object: " + path, e);
        }
    }
    
    @Override
    public boolean objectExists(String path) throws IOException {
        try {
            client.getObject(path).get(timeoutSeconds, TimeUnit.SECONDS);
            return true;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException && 
                e.getCause().getMessage().contains("Failed to retrieve object")) {
                return false;
            }
            throw new IOException("Error checking if object exists: " + path, e);
        } catch (InterruptedException | TimeoutException e) {
            throw new IOException("Error checking if object exists: " + path, e);
        }
    }
    
    @Override
    public void deleteObject(String path) throws IOException {
        try {
            client.deleteObject(path).get(timeoutSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new IOException("Failed to delete object: " + path, e);
        }
    }
    
    @Override
    public List<String> listObjects(String prefix) throws IOException {
        try {
            return client.listObjects(prefix).get(timeoutSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new IOException("Failed to list objects with prefix: " + prefix, e);
        }
    }
} 