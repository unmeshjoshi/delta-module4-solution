package deltajava.objectstore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for generating test data for delta table tests.
 */
public class TestDataGenerator {
    /**
     * Creates a list of sample customer records for testing with the specified count.
     *
     * @param count The number of customers to create
     * @return List of customer records as maps
     */
    public static List<Map<String, String>> createSampleCustomers(int count) {
        List<Map<String, String>> customers = new ArrayList<>();
        
        for (int i = 1; i <= count; i++) {
            customers.add(createCustomer(
                String.valueOf(i),
                "Customer " + i,
                "customer" + i + "@example.com",
                String.valueOf(25 + i % 40), // Ages 26-65
                "City " + i
            ));
        }
        
        return customers;
    }

    public static Map<String, String> createCustomer(String id, String name, String email, String age, String city) {
        Map<String, String> customer = new HashMap<>();
        customer.put("id", id);
        customer.put("name", name);
        customer.put("email", email);
        customer.put("age", age);
        customer.put("city", city);
        return customer;
    }
} 