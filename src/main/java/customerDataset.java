import java.util.Random;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

// Rules:
// The Customers dataset should have the following attributes for each customer:
// ID: unique sequential number (integer) from 1 to 50,000 (that is the file will have 50,000 line)
// Name: random sequence of characters of length between 10 and 20 (do not include commas)
// Age: random number (integer) between 10 to 70
// Gender: string that is either “male” or “female”
// CountryCode: random number (integer) between 1 and 10
// Salary: random number (float) between 100 and 10000

public class customerDataset {
    private static final int NUM_CUSTOMERS = 50000;
    private static final Random RANDOM = new Random();
    private static final String CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    public static void main(String[] args) {
        String outputFile = "customers.csv";

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            for (int id = 1; id <= NUM_CUSTOMERS; id++) {
                String name = randomName(10, 20);
                int age = randomInt(10, 70);
                String gender = RANDOM.nextBoolean() ? "male" : "female";
                int countryCode = randomInt(1, 10);
                float salary = randomFloat(100f, 10000f);

                writer.write(String.format("%d,%s,%d,%s,%d,%.2f", id, name, age, gender, countryCode, salary));
                writer.newLine();
            }
            System.out.println("Created customers.csv dataset.");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // Public to allow use in the transactionDataset
    public static String randomName(int minLen, int maxLen) {
        int length = randomInt(minLen, maxLen);
        StringBuilder sb = new StringBuilder(length);

        for (int i = 0; i < length; i++) {
            sb.append(CHARS.charAt(RANDOM.nextInt(CHARS.length())));
        }
        return sb.toString();
    }

    public static int randomInt(int min, int max) {
        return RANDOM.nextInt(max - min + 1) + min;
    }

    public static float randomFloat(float min, float max) {
        return min + RANDOM.nextFloat() * (max - min);
    }
}

