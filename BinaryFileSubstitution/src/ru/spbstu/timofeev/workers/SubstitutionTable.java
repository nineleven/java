package ru.spbstu.timofeev.workers;

import ru.spbstu.pipeline.RC;
import ru.spbstu.timofeev.utils.FileParser;
import ru.spbstu.timofeev.utils.Pair;
import ru.spbstu.timofeev.utils.PipelineBaseGrammar;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class SubstitutionTable {

    private final HashMap<Byte, Byte> table;

    private final Logger logger;

    private SubstitutionTable(HashMap<Byte, Byte> table, Logger logger) {
        this.table = table;
        this.logger = logger;
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

    public RC Substitute(byte[] data) {

        if (data == null) {
            logger.warning("Invalid substitution input");
            return RC.CODE_INVALID_ARGUMENT;
        }

        for (int i = 0; i < data.length; ++i) {
            data[i] = Substitute(data[i]);
        }

        return RC.CODE_SUCCESS;
    }

    private static boolean isValidTable(HashMap<Byte, Byte> map) {
        boolean[] present = new boolean[256];

        for(Map.Entry<Byte, Byte> entry: map.entrySet()) {
            if (!map.containsKey(entry.getValue())) {
                return false;
            }
            if (present[128 + entry.getValue()]) {
                return false;
            }
            present[128 + entry.getValue()] = true;
        }

        return true;
    }

    public static Pair<SubstitutionTable, RC> fromFile(String filename, Logger logger) {

        PipelineBaseGrammar tableGrammar = new PipelineBaseGrammar(new String[] {}) {
            private final String delimiter = "=>";

            @Override
            public String delimiter() {
                return delimiter;
            }

            @Override
            public boolean containsToken(String token) {
                return true;
            }
        } ;

        Pair<HashMap<String, String>, RC> res = FileParser.readMap(
                filename, tableGrammar, logger
        );
        if (res.first == null) {
            logger.severe("Failed to read a table from " + filename);
            return new Pair<>(null, res.second);
        }

        HashMap<String, String> stringTable = res.first;

        HashMap<Byte, Byte> byteTable = StringTableToByte(stringTable);
        if (byteTable == null) {
            logger.severe("Failed to convert table to byte");
            return new Pair<>(null, RC.CODE_CONFIG_SEMANTIC_ERROR);
        }

        if (!isValidTable(byteTable)) {
            logger.severe("Invalid mapping");
            return new Pair<>(null, RC.CODE_CONFIG_SEMANTIC_ERROR);
        }

        SubstitutionTable table = new SubstitutionTable(byteTable, logger);

        return new Pair<>(table, RC.CODE_SUCCESS);
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
            result = (byte) Integer.parseInt(line.substring(2), 16);
        } catch (NumberFormatException ex) {
            return null;
        }

        return result;
    }
}