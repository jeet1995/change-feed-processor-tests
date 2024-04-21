package com.cfp.test;

import com.beust.jcommander.JCommander;
import com.cfp.test.runners.ChangeFeedRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    private static final ExecutorService cfpExecutorService = new ScheduledThreadPoolExecutor(1);

    public static void main(String[] args) {

        final Configuration cfg = new Configuration();
        cfg.populateWithDefaults();

        logger.info("Parsing command-line args...");

        JCommander jCommander = new JCommander(cfg, null, args);

        ChangeFeedRunner changeFeedRunner = new ChangeFeedRunner();

        Future<?> task = cfpExecutorService.submit(() -> changeFeedRunner.execute(cfg));

        while (true) {
            if (task.isDone()) {
                cfpExecutorService.shutdown();
                break;
            }
        }
    }
}
