package com.cfp.runners;

import com.azure.cosmos.ChangeFeedProcessor;
import com.azure.cosmos.ChangeFeedProcessorBuilder;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.models.ChangeFeedProcessorOptions;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.ThroughputProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ChangeFeedProcessorRunner {

    private static final Logger logger = LoggerFactory.getLogger(ChangeFeedProcessorRunner.class);

    private static final ExecutorService bulkIngestionExecutorService = new ScheduledThreadPoolExecutor(1);

    public void execute(Configuration cfg) {

        int docCountToRead = cfg.getDocCountToIngestBeforeSplit() + cfg.getDocCountToIngestAfterSplit();
        Set<String> idsFetched = ConcurrentHashMap.newKeySet();
        AtomicReference<Instant> cfpStartTime = new AtomicReference<>();
        AtomicReference<Instant> cfpEndTime = new AtomicReference<>();
        AtomicBoolean isProcessingComplete = new AtomicBoolean(false);

        String databaseId = cfg.getDatabaseId();
        String feedContainerId = UUID.randomUUID() + "-" + "feed";
        String leaseContainerId = UUID.randomUUID() + "-" + "lease";

        CosmosAsyncDatabase ffcfDatabase = null;
        CosmosAsyncContainer feedContainer = null, leaseContainer = null;
        final AtomicReference<BulkIngestionAndSplitRunner> bulkIngestionAndSplitRunner = new AtomicReference<>();

        try (CosmosAsyncClient asyncClient = buildCosmosAsyncClient(cfg)) {

            ffcfDatabase = createDatabaseIfNotExists(asyncClient, databaseId);

            logger.info("Building feed container and lease container...");

            feedContainer = createContainerIfNotExists(ffcfDatabase, feedContainerId, cfg.getFeedContainerInitialThroughput(), "/mypk");
            leaseContainer = createContainerIfNotExists(ffcfDatabase, leaseContainerId, 400, "/id");

            List<ChangeFeedProcessor> changeFeedProcessors = new ArrayList<>();

            for (int i = 0; i < 1; i++) {
                ChangeFeedProcessor changeFeedProcessor = buildChangeFeedProcessor(
                    "HOST" + "_" + i,
                    feedContainer,
                    leaseContainer,
                    cfpStartTime,
                    cfpEndTime,
                    idsFetched,
                    isProcessingComplete,
                    docCountToRead);

                changeFeedProcessors.add(changeFeedProcessor);
            }

            bulkIngestionAndSplitRunner.set(new BulkIngestionAndSplitRunner());

            Flux
                .fromIterable(changeFeedProcessors)
                .flatMap(changeFeedProcessor -> changeFeedProcessor.start())
                .collectList()
                .doOnSuccess(unused -> bulkIngestionExecutorService.submit(() -> bulkIngestionAndSplitRunner.get().execute(cfg, feedContainerId)))
                .block();

            while (!isProcessingComplete.get()) {
                // do nothing
            }

            Flux
                .fromIterable(changeFeedProcessors)
                .flatMap(changeFeedProcessor -> changeFeedProcessor.stop())
                .collectList()
                .doOnSuccess(unused -> bulkIngestionExecutorService.shutdown())
                .block();

            Thread.sleep(30_000);

        } catch (Exception ex) {
            logger.error("Exception occurred : {}", ex.toString());
        } finally {

            if (feedContainer != null) {
                logger.info("Deleting feed container.");
                feedContainer.delete().block();
            }

            if (leaseContainer != null) {
                logger.info("Deleting lease container.");
                leaseContainer.delete().block();
            }

            if (ffcfDatabase != null) {
                logger.info("Deleting FFCF database");
                ffcfDatabase.delete().block();
            }

            printDurations(
                bulkIngestionAndSplitRunner.get().getSplitStartTime().get(),
                bulkIngestionAndSplitRunner.get().getSplitEndTime().get(),
                bulkIngestionAndSplitRunner.get().getIngestionStartTime().get(),
                bulkIngestionAndSplitRunner.get().getIngestionEndTime().get(),
                cfpStartTime.get(),
                cfpEndTime.get());
        }
    }

    private static CosmosAsyncClient buildCosmosAsyncClient(Configuration cfg) {

        CosmosAsyncClient client = new CosmosClientBuilder()
            .endpoint(cfg.getServiceEndpoint())
            .key(cfg.getMasterKey())
            .contentResponseOnWriteEnabled(true)
            .buildAsyncClient();

        return client;
    }

    private static ChangeFeedProcessor buildChangeFeedProcessor(
        String hostName,
        CosmosAsyncContainer feedContainer,
        CosmosAsyncContainer leaseContainer,
        final AtomicReference<Instant> startTime,
        final AtomicReference<Instant> endTime,
        final Set<String> idsFetched,
        final AtomicBoolean isProcessingComplete,
        final int idCountsToFetch) {

        return new ChangeFeedProcessorBuilder()
            .hostName(hostName)
            .feedContainer(feedContainer)
            .leaseContainer(leaseContainer)
            .handleAllVersionsAndDeletesChanges((docs, context) -> {

                if (idsFetched.isEmpty()) {
                    startTime.set(Instant.now());
                }

                for (com.azure.cosmos.models.ChangeFeedProcessorItem doc : docs) {
                    idsFetched.add(doc.getCurrent().get("id").asText());
                }

                logger.info("Ids fetched : {}", idsFetched.size());

                if (idsFetched.size() >= idCountsToFetch) {
                    isProcessingComplete.set(true);
                    endTime.set(Instant.now());
                }

                logger.info("Lease token : {}", context.getLeaseToken());
            })
            .options(new ChangeFeedProcessorOptions()
                .setLeasePrefix("TEST")
                .setStartFromBeginning(false)
                .setMaxItemCount(10_000)
            )
            .buildChangeFeedProcessor();
    }

    private static CosmosAsyncDatabase createDatabaseIfNotExists(CosmosAsyncClient asyncClient, String databaseId) {
        asyncClient.createDatabaseIfNotExists(databaseId).block();

        return asyncClient.getDatabase(databaseId);
    }

    private static CosmosAsyncContainer createContainerIfNotExists(CosmosAsyncDatabase asyncDatabase, String containerId, int throughput, String pkPath) {
        CosmosContainerProperties containerProperties = new CosmosContainerProperties(containerId, pkPath);
        asyncDatabase.createContainerIfNotExists(containerProperties, ThroughputProperties.createManualThroughput(throughput)).block();

        return asyncDatabase.getContainer(containerId);
    }

    private static void printDurations(
        Instant splitStartTime,
        Instant splitEndTime,
        Instant ingestionStartTime,
        Instant ingestionEndTime,
        Instant cfpStartTime,
        Instant cfpEndTime) {

        logger.info("Split duration : {}", Duration.between(splitStartTime, splitEndTime).toMillis());
        logger.info("Bulk ingestion duration : {}", Duration.between(ingestionStartTime, splitStartTime).plus(Duration.between(splitEndTime, ingestionEndTime)).toMillis());
        logger.info("CFP ingestion duration : {}", Duration.between(cfpStartTime, cfpEndTime).minus(Duration.between(splitStartTime, splitEndTime)).toMillis());
    }
}
