import java.util.HashMap;
import java.util.Map;

public class SubstitutionTable {

    HashMap<Byte, Byte> table;

    private SubstitutionTable(HashMap<Byte, Byte> table) {
        this.table = table;
    }

    public byte Substitute(byte x) {
        Byte y = table.get(x);
        if (y == null) {
            return x;
        }
        else {
            return y;
        }
    }

    /*
     reads a table from file, each nonempty line of which is in the following format:
     0x##[any number of space characters]0x## (# stands for any hexadecimal digit)
     returns null in case of IO exception or if a line of wrong format was found
     */
    public static SubstitutionTable fromFile(String filename) {

        HashMap<String, String> stringTable = FileParser.readMap(filename, "");
        if (stringTable == null) {
            return null;
        }

        HashMap<Byte, Byte> byteTable = StringTableToByte(stringTable);
        if (byteTable == null) {
            return null;
        }

        return new SubstitutionTable(byteTable);
    }

    private static HashMap<Byte, Byte> StringTableToByte(HashMap<String, String> stringTable) {
        HashMap<Byte, Byte> byteTable = new HashMap<>();

        for (Map.Entry<String, String> entry:stringTable.entrySet()) {
            Byte key = parseByte(entry.getKey());
            Byte value = parseByte(entry.getValue());
            if (key == null || value == null) {
                return null;
            }
            byteTable.put(key, value);
        }

        return byteTable;
    }

    /*
     returns null if unable to parse correctly
     */
    private static Byte parseByte(String line) {

        if (line == null || line.length() != 4 ||
                !line.startsWith("0x")) {
            return null;
        }

        byte result;

        try {
            // we don't want to deal with signed bytes in table file,
            // yet in java byte is signed type, so we have to read
            // the value into integer first
            int integerRepresentation = Integer.parseInt(line.substring(2), 16);
            result = (byte) (integerRepresentation - 128);
        } catch (NumberFormatException ex) {
            return null;
        }

        return result;
    }
}