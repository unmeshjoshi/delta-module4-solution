package deltajava.objectstore.delta.storage;

/**
 * Represents a Delta Log filename with operations to extract and validate version information.
 * Delta log files typically follow a pattern like "00000000000000000001.json"
 */
public class LogFileName {
    private final String fileName;
    private final long version;
    private static final String FILE_EXTENSION = ".json";
    
    /**
     * Creates a DeltaLogFileName from a complete file path or filename
     * 
     * @param fileName The file path or name
     * @throws IllegalArgumentException if the file is not a valid Delta log file
     */
    public LogFileName(String fileName) {
        // Extract just the filename if a path is provided
        String baseName = fileName;
        int lastSlashIndex = fileName.lastIndexOf("/");
        if (lastSlashIndex >= 0) {
            baseName = fileName.substring(lastSlashIndex + 1);
        }
        
        this.fileName = baseName;
        
        if (!isValidLogFile()) {
            throw new IllegalArgumentException("Invalid Delta log file name: " + baseName);
        }
        
        this.version = extractVersion();
    }
    
    /**
     * Creates a DeltaLogFileName from a version number
     * 
     * @param version The version number
     * @return A new DeltaLogFileName instance
     */
    public static LogFileName fromVersion(long version) {
        if (version < 0) {
            throw new IllegalArgumentException("Version cannot be negative");
        }
        
        // Format: 20 digits with leading zeros + .json
        String fileName = String.format("%020d", version) + FILE_EXTENSION;
        return new LogFileName(fileName);
    }
    
    /**
     * Checks if this is a valid Delta log file
     * 
     * @return true if the filename follows the Delta log file pattern
     */
    public boolean isValidLogFile() {
        return fileName != null && fileName.endsWith(FILE_EXTENSION) && 
               fileName.length() > FILE_EXTENSION.length() && 
               isNumeric(fileName.substring(0, fileName.length() - FILE_EXTENSION.length()));
    }
    
    /**
     * Extracts the version number from the filename
     * 
     * @return the version number
     */
    private long extractVersion() {
        if (!isValidLogFile()) {
            return -1;
        }
        
        String versionString = fileName.substring(0, fileName.length() - FILE_EXTENSION.length());
        return Long.parseLong(versionString);
    }
    
    /**
     * Gets the version number from this log file
     * 
     * @return the version number
     */
    public long getVersion() {
        return version;
    }
    
    /**
     * Gets the file name (without path)
     * 
     * @return the file name
     */
    public String getFileName() {
        return fileName;
    }
    
    /**
     * Combines this filename with a directory path
     * 
     * @param directory The directory path
     * @return The full path to the file
     */
    public String getPathIn(String directory) {
        if (!directory.endsWith("/")) {
            directory += "/";
        }
        return directory + fileName;
    }
    
    /**
     * Checks if a string contains only numeric characters
     * 
     * @param str The string to check
     * @return true if the string is numeric
     */
    private boolean isNumeric(String str) {
        if (str == null || str.isEmpty()) {
            return false;
        }
        
        for (char c : str.toCharArray()) {
            if (!Character.isDigit(c)) {
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * Extracts version from a filename without creating an object.
     * Returns -1 if the filename is not a valid delta log file.
     * 
     * @param fileName The file path or name to extract version from
     * @return The version number, or -1 if not a valid log file
     */
    public static long versionFromName(String fileName) {
        try {
            // Extract just the filename if a path is provided
            String baseName = fileName;
            int lastSlashIndex = fileName.lastIndexOf("/");
            if (lastSlashIndex >= 0) {
                baseName = fileName.substring(lastSlashIndex + 1);
            }
            
            if (baseName != null && baseName.endsWith(FILE_EXTENSION) &&
                baseName.length() > FILE_EXTENSION.length()) {
                
                String versionString = baseName.substring(0, baseName.length() - FILE_EXTENSION.length());
                if (isNumericStatic(versionString)) {
                    return Long.parseLong(versionString);
                }
            }
            return -1;
        } catch (Exception e) {
            return -1;
        }
    }
    
    /**
     * Static version of isNumeric for use in static methods
     */
    private static boolean isNumericStatic(String str) {
        if (str == null || str.isEmpty()) {
            return false;
        }
        
        for (char c : str.toCharArray()) {
            if (!Character.isDigit(c)) {
                return false;
            }
        }
        
        return true;
    }
    
    @Override
    public String toString() {
        return fileName;
    }
} 