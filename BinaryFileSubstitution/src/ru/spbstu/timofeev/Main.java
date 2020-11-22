package ru.spbstu.timofeev;

import ru.spbstu.pipeline.RC;

import java.util.logging.Logger;

public class Main {

    public static void main(String[] Args)  {
        Logger logger = Logger.getLogger("Logger");

        if (Args == null || Args.length != 1) {
            logger.severe("Expected one command-line argument");
            return;
        }
        String configFileName = Args[0];

        PipelineManager manager = PipelineManager.createInstance(configFileName, logger);
        if (manager == null) {
            logger.severe("Failed to create a pipeline manager");
            return;
        }

        RC retCode = manager.run();
        if (retCode != RC.CODE_SUCCESS) {
            logger.severe("Pipeline execution failed");
            return;
        }

        logger.info("DONE");
    }
}
