package deltajava.core;

/**
 * A basic implementation of the Partition interface that simply stores 
 * a partition index. This can be used for RDDs that don't need specialized
 * partition information.
 */
public class BasePartition implements Partition {
    private final int partitionId;

    /**
     * Create a new BasePartition with the given index.
     * 
     * @param partitionId The partition index
     */
    public BasePartition(int partitionId) {
        this.partitionId = partitionId;
    }

    @Override
    public int index() {
        return partitionId;
    }
    
    @Override
    public String toString() {
        return "Partition " + partitionId;
    }
} 