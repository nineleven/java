import java.io.*;
import java.util.HashMap;

public class FileParser {

    private static void closeStream(Closeable stream) {
        try {
            stream.close();
        }
        catch (IOException ex) {
            // net tak net
        }
    }

    /*
     reads key value pairs from file in to a hashmap
     each string of file except empty strings should be in the following format:
     key[any number of space characters]delimiter[any number of space characters]value
     returns null in case of IO exception or if a line of wrong format was found
     */
    public static HashMap<String, String> readMap(String filename, String delimiter) {
        FileInputStream inputStream;
        try {
            inputStream = new FileInputStream(filename);
        } catch (FileNotFoundException ex) {
            return null;
        }

        HashMap<String, String> map = new HashMap<>();

        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        try {
            String line;

            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }

                Pair<String, String> keyValPair = parseKeyValue(line, delimiter);
                if (keyValPair == null) {
                    // failed to extract key-value pair from this line
                    // it might be better to return null here, might not
                    continue;
                }
                map.put(keyValPair.first, keyValPair.second);
            }
        } catch(IOException ex) {
            return null;
        }
        finally {
            closeStream(reader);
        }

        return map;
    }

    private static Pair<String, String> parseKeyValue(String line, String delimiter) {
        String[] split = line.split(delimiter);

        if (split.length != 2) {
            return null;
        }
        else {
            return new Pair<>(split[0].trim(), split[1].trim());
        }
    }
}