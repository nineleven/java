package ru.spbstu.timofeev;

import ru.spbstu.pipeline.RC;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class Main {

    static final String logFileName = "log.txt";

    private static Logger makeLogger() {
        Logger logger = Logger.getLogger("Logger");
        FileHandler fh;
        try {
            fh = new FileHandler(logFileName);
        } catch (IOException ex) {
            return null;
        }
        SimpleFormatter sf = new SimpleFormatter();
        fh.setFormatter(sf);
        logger.addHandler(fh);
        logger.setUseParentHandlers(false);

        return logger;
    }

    private static void printResult(RC retCode) {
        switch (retCode){
            case CODE_CONFIG_GRAMMAR_ERROR:
                System.out.println("Grammar error, see details in " + logFileName);
                break;
            case CODE_CONFIG_SEMANTIC_ERROR:
                System.out.println("Semantic error, see details in " + logFileName);
                break;
            case CODE_FAILED_PIPELINE_CONSTRUCTION:
                System.out.println("Failed to construct a pipeline, see details in " + logFileName);
                break;
            case CODE_FAILED_TO_READ:
                System.out.println("Reading error, see details in " + logFileName);
                break;
            case CODE_FAILED_TO_WRITE:
                System.out.println("Writing error, see details in " + logFileName);
                break;
            case CODE_INVALID_ARGUMENT:
                System.out.println("Invalid argument received, see details in " + logFileName);
                break;
            case CODE_INVALID_INPUT_STREAM:
                System.out.println("Got invalid input stream, see details in " + logFileName);
                break;
            case CODE_INVALID_OUTPUT_STREAM:
                System.out.println("Got invalid output stream, see details in " + logFileName);
                break;
            case CODE_SUCCESS:
                System.out.println("Done");
                break;
            default:
                System.out.println("Unknown returning code " + retCode + ", see details in " + logFileName);
                break;
        }
    }

    public static void main(String[] Args)  {
        Logger logger = makeLogger();
        if (logger == null) {
            System.out.println("Failed to initialize a logger");
            return;
        }

        if (Args == null || Args.length != 1) {
            logger.severe("Expected one command-line argument");
            printResult(RC.CODE_INVALID_ARGUMENT);
            return;
        }
        String configFileName = Args[0];

        PipelineManager manager = PipelineManager.createInstance(configFileName, logger);
        if (manager == null) {
            logger.severe("Failed to create a pipeline manager");
            printResult(RC.CODE_FAILED_PIPELINE_CONSTRUCTION);
            return;
        }

        RC retCode = manager.run();
        if (retCode != RC.CODE_SUCCESS) {
            logger.severe("Pipeline execution failed");
            printResult(retCode);
            return;
        }

        logger.info("DONE");
        printResult(retCode);
    }
}
