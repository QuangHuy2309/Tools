
import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

    public class SortLinesInFile {
        public static void main(String[] args) {
            // Input and output file paths
            String inputFile = "src/resources/input/listTopic.txt"; // Replace with your input file path
            String outputFile = "src/resources/output/listSortedTopic.txt"; // Replace with your output file path

            try {
                // Read lines from the input file
                List<String> lines = readLinesFromFile(inputFile);

                // Sort the lines alphabetically
                Collections.sort(lines);

                // Write the sorted lines to the output file
                writeLinesToFile(outputFile, lines);

                System.out.println("Lines sorted and written to " + outputFile);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // Read lines from a file into a List
        private static List<String> readLinesFromFile(String filePath) throws IOException {
            List<String> lines = new ArrayList<>();
            BufferedReader reader = new BufferedReader(new FileReader(filePath));

            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }

            reader.close();
            return lines;
        }

        // Write lines to a file
        private static void writeLinesToFile(String filePath, List<String> lines) throws IOException {
            BufferedWriter writer = new BufferedWriter(new FileWriter(filePath));

            for (String line : lines) {
                writer.write(line);
                writer.newLine();
            }

            writer.close();
        }
    }

