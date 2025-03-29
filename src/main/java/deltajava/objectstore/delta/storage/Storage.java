package deltajava.objectstore.delta.storage;

import java.io.IOException;
import java.util.List;

/**
 * Defines core operations for interacting with storage systems.
 * This abstraction allows Delta functionality to work with different
 * underlying storage implementations.
 */
public interface Storage {
    /**
     * Reads an object from the storage.
     * 
     * @param path Path to the object
     * @return The object data as a byte array
     * @throws IOException If the object cannot be read
     */
    byte[] readObject(String path) throws IOException;
    
    /**
     * Writes an object to the storage.
     * 
     * @param path Path where the object should be stored
     * @param data The data to write
     * @throws IOException If the object cannot be written
     */
    void writeObject(String path, byte[] data) throws IOException;
    
    /**
     * Checks if an object exists in the storage.
     * 
     * @param path Path to check
     * @return true if the object exists, false otherwise
     * @throws IOException If the existence check fails
     */
    boolean objectExists(String path) throws IOException;
    
    /**
     * Deletes an object from the storage.
     * 
     * @param path Path to the object to delete
     * @throws IOException If the deletion fails
     */
    void deleteObject(String path) throws IOException;
    
    /**
     * Lists objects in the storage with the given prefix.
     * 
     * @param prefix Prefix to filter objects by
     * @return List of object paths
     * @throws IOException If the listing fails
     */
    List<String> listObjects(String prefix) throws IOException;
} 