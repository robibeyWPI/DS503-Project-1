import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class transactionDataset {
    private static final int NUM_TRANS = 5_000_000;
    private static final int NUM_CUSTOMERS = 50000;

    public static void main(String[] args) {
        String outputFile = "transactions.csv";

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            for (int transID = 1; transID <= NUM_TRANS; transID++) {
                int custID = customerDataset.randomInt(1, NUM_CUSTOMERS);
                float transTotal = customerDataset.randomFloat(10f, 1000f);
                int transNumItems = customerDataset.randomInt(1, 10);
                String transDesc = customerDataset.randomName(20, 50);

                writer.write(String.format("%d,%d,%.2f,%d,%s", transID, custID, transTotal, transNumItems, transDesc));
                writer.newLine();
            }
            System.out.println("Created transactions.csv dataset.");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
