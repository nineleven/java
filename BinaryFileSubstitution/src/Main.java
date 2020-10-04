import java.io.*;

public class Main {

    private static int Substitute(Config cfg) {
        String inputFilename = cfg.getParameter("input_file");
        String outputFilename = cfg.getParameter("output_file");
        String tableFilename = cfg.getParameter("table_file");

        if (inputFilename == null || outputFilename== null ||
                tableFilename == null) {
            // bad config
            return 1;
        }

        SubstitutionTable table = SubstitutionTable.fromFile(tableFilename);

        if (table == null) {
            // failed to construct a substitution table
            return 2;
        }

        Substitutor substitutor = new Substitutor(table);

        int retCode = substitutor.FileToFile(inputFilename, outputFilename);

        if (retCode != 0) {
            // IO exception while processing files
            return 3;
        }

        return 0;
    }

    public static void main(String[] Args)  {
//        if (Args == null || Args.length != 1) {
//            System.out.println("Expected one command-line argument.");
//            return;
//        }
//        String configFileName = Args[0];
        String configFileName = "./config.txt";

        Config cfg = Config.fromFile(configFileName);
        if (cfg == null) {
            System.out.println("Failed to read config file " + configFileName + ".");
            return;
        }

        int code = Substitute(cfg);

        switch (code) {
            case 0:
                System.out.println("Done.");
                break;
            case 1:
                System.out.println("Bad config.");
                break;
            case 2:
                System.out.println("Failed to read a substitution table.");
                break;
            case 3:
                System.out.println("IO exception while processing files.");
                break;
            default:
                System.out.println("Unknown exception.");
                break;
        }
    }
}
