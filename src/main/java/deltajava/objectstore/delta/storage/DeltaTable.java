package deltajava.objectstore.delta.storage;


import deltajava.objectstore.delta.storage.actions.Action;
import deltajava.objectstore.delta.storage.actions.AddFile;
import deltajava.objectstore.delta.storage.util.ParquetUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;

/**
 * Represents a Delta table which is a directory containing data files and transaction logs.
 * This implementation provides a simplified version of the Delta Lake protocol.
 */
public class DeltaTable {
    
    private final String tablePath;
    private final DeltaLog deltaLog;
    
    /**
     * Creates a new Delta table at the specified path.
     *
     * @param tablePath the path where the table will be stored
     * @throws IOException if an I/O error occurs
     */
    public DeltaTable(Storage storage, String tablePath) throws IOException {
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
        
        // Create the full path to the data file
        Path dataFilePath = Paths.get(tablePath, "data", fileName);
        
        // Write the records to a Parquet file
        long fileSize = ParquetUtil.writeRecords(records, dataFilePath);


        // Create an AddFile action
        AddFile addFile = new AddFile(
                "data/" + fileName,
                fileSize,
                timestamp);
        
        return List.of(addFile);
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
            Path filePath = Paths.get(tablePath.toString(), addFile.getPath());
            List<Map<String, String>> fileRecords = ParquetUtil.readRecords(filePath);
            allRecords.addAll(fileRecords);
        }

        return allRecords;
    }
}