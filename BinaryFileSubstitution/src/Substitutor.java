import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class Substitutor {

    SubstitutionTable table;

    public Substitutor(SubstitutionTable table) {
        this.table = table;
    }

    public int FileToFile(String inputFilename, String outputFilename) {

        FileInputStream inputStream;
        FileOutputStream outputStream;
        try {
            inputStream = new FileInputStream(inputFilename);
            outputStream = new FileOutputStream(outputFilename);
        } catch(FileNotFoundException | NullPointerException ex) {
            // unable to open files
            return 1;
        }

        int inputByteIntRepr;
        try {
            while ((inputByteIntRepr = inputStream.read()) != -1) {
                byte inputByte = (byte) (inputByteIntRepr - 128);
                byte outputByte = table.Substitute(inputByte);
                int intRepr = outputByte + 128;
                outputStream.write(intRepr);
            }
        } catch (IOException ex) {
            // IO exception while processing files
            return 2;
        }

        return 0;
    }
}
