import java.io.*;

enum ErrorCode {
    ERROR_OK,
    ERROR_IO
}

class InputFileReader {

    private FileInputStream stream;
    private String filename;

    public InputFileReader(String filename) {
        this.filename = filename;
    }

    public int readBytes(byte[] buffer, int numBytes) {
        if (stream == null) {
            try {
                stream = new FileInputStream(filename);
            }
            catch (FileNotFoundException ex) {
                return -1;
            }
        }
        int bytesRead = 0;
        try {
            bytesRead = stream.read(buffer, 0, numBytes);
            if (bytesRead == -1) {
                bytesRead = 0;
            }
        }
        catch (IOException ex) {
            return -1;
        }

        return bytesRead;
    }

    public void close() {
        try {
            stream.close();
        }
        catch (IOException ex) {

        }
    }
}

class OutputFileWriter {
    private FileOutputStream stream;
    private String filename;

    public OutputFileWriter(String filename) {
        this.filename = filename;
    }

    public boolean writeBytes(byte[] bytes) {
        if (stream == null) {
            try {
                stream = new FileOutputStream(filename);
            }
            catch (FileNotFoundException ex) {
                return false;
            }
        }

        try {
            stream.write(bytes);
        }
        catch (IOException ex) {
            return false;
        }

        return true;
    }

    public void close() {
        try {
            stream.close();
        }
        catch (IOException ex) { }
    }
}

public class Substitutor {

    SubstitutionTable table;

    public Substitutor(SubstitutionTable table) {
        this.table = table;
    }

    public ErrorCode FileToFile(String inputFilename, String outputFilename, int bufferSize) {

        InputFileReader reader = new InputFileReader(inputFilename);
        OutputFileWriter writer = new OutputFileWriter(outputFilename);

        byte[] buffer = new byte[bufferSize];

        int bytesRead = reader.readBytes(buffer, bufferSize);

        while (bytesRead > 0) {
            boolean result = writer.writeBytes(table.Substitute(buffer, bytesRead));

            if (!result) {
                bytesRead = -1;
                break;
            }

            bytesRead = reader.readBytes(buffer, bufferSize);
        }

        reader.close();
        writer.close();

        if (bytesRead < 0) {
            return ErrorCode.ERROR_IO;
        }
        else {
            return ErrorCode.ERROR_OK;
        }
    }
}
