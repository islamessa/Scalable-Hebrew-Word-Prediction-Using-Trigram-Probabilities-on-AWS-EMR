import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class tast{
    public static void main(String[] args) {
        // Path to the input file
        String inputFilePath = "textToSort.txt";  // Replace with your file path
        // Path to the output file
        String outputFilePath = "testout.txt";  // Output file name

        // Number of lines to read


        // Try to read and write to files
        try (BufferedReader br = new BufferedReader(new FileReader(inputFilePath));
             PrintWriter pw = new PrintWriter(new FileWriter(outputFilePath))) {

            String line;

            // Read and write lines
            while ((line = br.readLine()) != null ) {
                pw.println(line);  // Write the line to the output file
            }

            // Check if the file had fewer than 10 lines

        } catch (IOException e) {
            System.err.println("Error reading or writing to the file: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
