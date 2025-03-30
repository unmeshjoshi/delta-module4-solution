package deltajava.objectstore.delta.storage;


import deltajava.objectstore.delta.storage.actions.Action;
import deltajava.objectstore.delta.storage.actions.AddFile;
import deltajava.objectstore.delta.storage.util.ParquetUtil;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;

/**
 * Represents a Delta table which is a directory containing data files and transaction logs.
 * This implementation provides a simplified version of the Delta Lake protocol.
 */
public class DeltaTable {

    private final Storage storage;
    private final String tablePath;
    private final DeltaLog deltaLog;
    
    /**
     * Creates a new Delta table at the specified path.
     *
     * @param tablePath the path where the table will be stored
     * @throws IOException if an I/O error occurs
     */
    public DeltaTable(Storage storage, String tablePath) throws IOException {
        this.storage = storage;
        this.tablePath = tablePath;
        this.deltaLog = new DeltaLog(storage, tablePath);

    }
    
    /**
     * Initializes a new Delta table with protocol and metadata.
     *
     * @throws IOException if an I/O error occurs
     */
    private void initialize() throws IOException {

    }

    /**
     * Starts a new optimistic transaction for this table.
     *
     * @return a new transaction
     * @throws IOException if an I/O error occurs
     */
    public OptimisticTransaction startTransaction() throws IOException {
        return deltaLog.startTransaction();
    }

    /**
     * Inserts records into the Delta table. Each record is a map of column names to values.
     *
     * @param records the records to insert
     * @return the number of records inserted
     * @throws IOException if an I/O error occurs
     */
    public List<Action> insert(List<Map<String, String>> records) throws IOException {
        if (records == null || records.isEmpty()) {
            return Collections.emptyList();
        }
        // Generate a unique file name
        String fileId = UUID.randomUUID().toString();
        long timestamp = Instant.now().toEpochMilli();
        String fileName = String.format("part-%s.parquet", fileId);
        String relativePath = "data/" + fileName;
        String fullStoragePath = tablePath + "/" + relativePath;
        
        // Create a temporary file locally
        java.nio.file.Path tempFilePath = java.nio.file.Files.createTempFile("delta-", ".parquet");
        try {
            // Write records to the temporary file
            long fileSize = ParquetUtil.writeRecords(records, tempFilePath);
            
            // Read the temporary file into memory
            byte[] fileData = java.nio.file.Files.readAllBytes(tempFilePath);
            
            // Write data to the actual storage
            storage.writeObject(fullStoragePath, fileData);
            
            // Create an AddFile action
            AddFile addFile = new AddFile(
                    relativePath,
                    fileSize,
                    timestamp);
            
            return List.of(addFile);
        } finally {
            // Delete the temporary file
            java.nio.file.Files.deleteIfExists(tempFilePath);
        }
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
        List<AddFile> activeFiles = deltaLog.snapshot().getAllFiles();

        // Read each file and collect records
        for (AddFile addFile : activeFiles) {
            String fullPath = tablePath + "/" + addFile.getPath();
            
            // Read the file data from storage
            byte[] fileData = storage.readObject(fullPath);
            
            // Create a temporary file to read with ParquetUtil
            java.nio.file.Path tempFilePath = java.nio.file.Files.createTempFile("delta-read-", ".parquet");
            
            try {
                // Write the data to the temporary file
                java.nio.file.Files.write(tempFilePath, fileData);
                
                // Read records from the temporary file
                List<Map<String, String>> fileRecords = ParquetUtil.readRecords(tempFilePath);
                allRecords.addAll(fileRecords);
            } finally {
                // Clean up the temporary file
                java.nio.file.Files.deleteIfExists(tempFilePath);
            }
        }

        return allRecords;
    }
}