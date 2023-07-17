import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class findFieldInScript {
    private static final String RESULT_FILE = "src/resources/output/FieldToFind_result.txt"; // Specify the path to the destination file

    public static void main (String[] args) {
        String fieldFile  = "src/resources/input/FieldToFind.txt"; // Specify the path to the file containing texts
        String scriptFile = "src/resources/input/ScriptToCompare.txt"; // Specify the path to the compare_file

        try {
            // Read the field names from the file
            Set<String> fields = readFieldsFromFile(fieldFile);

            // Read the table creation script
            String script = readScriptFromFile(scriptFile);

            // Check which fields appear in the script
            Set<String> matchedFields = findMatchedFields(fields, script);

            // Sort the matched fields alphabetically
            List<String> sortedFields = new ArrayList<>(matchedFields);
            Collections.sort(sortedFields);

            Set<String> previousResult = readPreviousResult();

            // Save the current result
            saveCurrentResult(matchedFields);


            // Print the sorted matched fields
            if ( sortedFields.isEmpty() ) {
                System.out.println("No matching fields found in the script.");
            }
            else {
                System.out.println(
                    "Matching fields found in the script (sorted alphabetically):" + sortedFields.size());
                for (String field : sortedFields) {
                    System.out.println(field);
                }
            }

            System.out.println("===============================");
            // Compare the current and previous results
            Set<String> addedFields = new HashSet<>(matchedFields);
            addedFields.removeAll(previousResult);

            Set<String> removedFields = new HashSet<>(previousResult);
            removedFields.removeAll(matchedFields);

            // Print the differences
            System.out.println("Number of added fields: " + addedFields.size());
            if (!addedFields.isEmpty()) {
                System.out.println("Added fields:");
                for (String field : addedFields) {
                    System.out.println("\t" + field);
                }
            }

            System.out.println("Number of removed fields: " + removedFields.size());
            if (!removedFields.isEmpty()) {
                System.out.println("Removed fields:");
                for (String field : removedFields) {
                    System.out.println("\t" + field);
                }
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static Set<String> readFieldsFromFile (String filePath) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        String         line;
        Set<String>    fields = new HashSet<>();

        while ( (line = reader.readLine()) != null ) {
            fields.add(line.trim());
        }

        reader.close();

        return fields;
    }

    private static String readScriptFromFile (String filePath) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        String         line;
        StringBuilder  sb     = new StringBuilder();

        while ( (line = reader.readLine()) != null ) {
            sb.append(line);
            sb.append(System.lineSeparator());
        }

        reader.close();

        return sb.toString();
    }

    private static Set<String> findMatchedFields (Set<String> fields, String script) {
        Set<String> matchedFields = new HashSet<>();

        // Convert the script to lowercase for case-insensitive matching
        String[] scriptTokens = script.split("\\W+");

        // Check each field if it appears in the script
        for (String field : fields) {
            if ( containsExactField(scriptTokens, field) ) {
                matchedFields.add(field);
            }
        }

        return matchedFields;
    }

    private static boolean containsExactField (String[] scriptTokens, String field) {
        for (String token : scriptTokens) {
            if ( token.equalsIgnoreCase(field) ) {
                return true;
            }
        }
        return false;

    }
    private static Set<String> readPreviousResult() throws IOException {
        Set<String> previousResult = new HashSet<>();

        File file = new File(RESULT_FILE);
        if (file.exists()) {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line;
            while ((line = reader.readLine()) != null) {
                previousResult.add(line.trim());
            }
            reader.close();
        }

        return previousResult;
    }

    private static void saveCurrentResult(Set<String> matchedFields) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(RESULT_FILE));
        for (String field : matchedFields) {
            writer.write(field);
            writer.newLine();
        }
        writer.close();
    }
}
