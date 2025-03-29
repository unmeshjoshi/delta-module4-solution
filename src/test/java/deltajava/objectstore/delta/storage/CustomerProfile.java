package deltajava.objectstore.delta.storage;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a customer profile for testing the Delta Lake implementation.
 */
public class CustomerProfile implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private final String id;
    private final String name;
    private final String email;
    private final int age;
    private final String region;
    private final double creditScore;
    private final boolean isPremium;
    private final String lastPurchaseDate;
    
    /**
     * Constructs a new CustomerProfile.
     *
     * @param id Customer ID
     * @param name Customer name
     * @param email Customer email
     * @param age Customer age
     * @param region Customer region
     * @param creditScore Customer credit score
     * @param isPremium Whether the customer is a premium member
     * @param lastPurchaseDate Date of last purchase
     */
    public CustomerProfile(String id, String name, String email, int age, 
                           String region, double creditScore, boolean isPremium, 
                           String lastPurchaseDate) {
        this.id = id;
        this.name = name;
        this.email = email;
        this.age = age;
        this.region = region;
        this.creditScore = creditScore;
        this.isPremium = isPremium;
        this.lastPurchaseDate = lastPurchaseDate;
    }
    
    /**
     * Converts this CustomerProfile to a Map for storage in Parquet files.
     *
     * @return A Map representation of this profile
     */
    public Map<String, String> toMap() {
        Map<String, String> map = new HashMap<>();
        map.put("id", id);
        map.put("name", name);
        map.put("email", email);
        map.put("age", String.valueOf(age));
        map.put("region", region);
        map.put("creditScore", String.valueOf(creditScore));
        map.put("isPremium", String.valueOf(isPremium));
        map.put("lastPurchaseDate", lastPurchaseDate);
        return map;
    }
    
    /**
     * Creates a CustomerProfile from a Map representation.
     *
     * @param map The Map representation
     * @return The CustomerProfile
     */
    public static CustomerProfile fromMap(Map<String, String> map) {
        return new CustomerProfile(
            map.get("id"),
            map.get("name"),
            map.get("email"),
            map.get("age") != null ? Integer.parseInt(map.get("age")) : 0,
            map.get("region"),
            map.get("creditScore") != null ? Double.parseDouble(map.get("creditScore")) : 0.0,
            map.get("isPremium") != null ? Boolean.parseBoolean(map.get("isPremium")) : false,
            map.get("lastPurchaseDate")
        );
    }
    
    // Getters
    
    public String getId() {
        return id;
    }
    
    public String getName() {
        return name;
    }
    
    public String getEmail() {
        return email;
    }
    
    public int getAge() {
        return age;
    }
    
    public String getRegion() {
        return region;
    }
    
    public double getCreditScore() {
        return creditScore;
    }
    
    public boolean isPremium() {
        return isPremium;
    }
    
    public String getLastPurchaseDate() {
        return lastPurchaseDate;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CustomerProfile that = (CustomerProfile) o;
        return age == that.age &&
               Double.compare(that.creditScore, creditScore) == 0 &&
               isPremium == that.isPremium &&
               Objects.equals(id, that.id) &&
               Objects.equals(name, that.name) &&
               Objects.equals(email, that.email) &&
               Objects.equals(region, that.region) &&
               Objects.equals(lastPurchaseDate, that.lastPurchaseDate);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(id, name, email, age, region, creditScore, isPremium, lastPurchaseDate);
    }
    
    @Override
    public String toString() {
        return "CustomerProfile{" +
               "id='" + id + '\'' +
               ", name='" + name + '\'' +
               ", email='" + email + '\'' +
               ", age=" + age +
               ", region='" + region + '\'' +
               ", creditScore=" + creditScore +
               ", isPremium=" + isPremium +
               ", lastPurchaseDate='" + lastPurchaseDate + '\'' +
               '}';
    }
} 