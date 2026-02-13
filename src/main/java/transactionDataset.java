import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

// Rules:
// The Transactions dataset should have the following attributes for each transaction:
// TransID: unique sequential number (integer) from 1 to 5,000,000 (the file has 5M transactions)
// CustID: References one of the customer IDs, i.e., from 1 to 50,000 (on Avg. a customer has 100 trans.)
// TransTotal: random number (float) between 10 and 1000
// TransNumItems: random number (integer) between 1 and 10
// TransDesc: random text of characters of length between 20 and 50 (do not include commas)

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
