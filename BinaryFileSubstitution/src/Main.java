
public class Main {

    private static int Substitute(Config cfg) {
        String inputFilename = cfg.getParameter(GlobalConstants.INPUT_FILE_CONFIG_FIELD);
        String outputFilename = cfg.getParameter(GlobalConstants.OUTPUT_FILE_CONFIG_FIELD);
        String tableFilename = cfg.getParameter(GlobalConstants.TABLE_FILE_CONFIG_FIELD);
        Integer bufferSize = cfg.getIntParameter(GlobalConstants.BUFFER_SIZE_FIELD);

        if (inputFilename == null || outputFilename== null ||
                tableFilename == null || bufferSize == null) {
            // bad config
            return 1;
        }

        SubstitutionTable table = SubstitutionTable.fromFile(tableFilename);

        if (table == null) {
            // failed to construct a substitution table
            return 2;
        }

        Substitutor substitutor = new Substitutor(table);

        ErrorCode retCode = substitutor.FileToFile(inputFilename, outputFilename, bufferSize.intValue());

        if (retCode != ErrorCode.ERROR_OK) {
            // IO exception while processing files
            return 3;
        }

        return 0;
    }

    private static void outputExecutionResults(int code) {
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

    public static void main(String[] Args)  {
//        if (Args == null || Args.length != 1) {
//            System.out.println("Expected one command-line argument.");
//            return;
//        }
//        String configFileName = Args[0];
        String configFileName = "./config.txt";

        Config cfg = Config.fromFile(configFileName, GlobalConstants.CONFIG_DELIMITER);
        if (cfg == null) {
            System.out.println("Failed to read config file " + configFileName + ".");
            return;
        }

        /*
        Числовые коды только в классе Main
         */
        int code = Substitute(cfg);

        outputExecutionResults(code);
    }
}
