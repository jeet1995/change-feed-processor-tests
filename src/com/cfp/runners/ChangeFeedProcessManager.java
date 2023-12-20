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
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ChangeFeedProcessManager {

    private static final Logger logger = LoggerFactory.getLogger(ChangeFeedProcessManager.class);

    private static final ExecutorService bulkIngestionExecutorService = new ScheduledThreadPoolExecutor(1);

    public void execute(Configuration cfg) {

        int docCountToRead = cfg.getDocCountToIngestBeforeSplit() + cfg.getDocCountToIngestAfterSplit();
        Set<String> idsFetched = ConcurrentHashMap.newKeySet();

        AtomicReference<Instant> cfpStartTime = new AtomicReference<>();
        AtomicReference<Instant> cfpEndTime = new AtomicReference<>();

        // todo (abhmohanty): replace with value holder
        AtomicReference<String> databaseResourceId = new AtomicReference<>();
        AtomicReference<String> feedCollectionResourceId = new AtomicReference<>();
        AtomicReference<String> leaseCollectionResourceId = new AtomicReference<>();

        AtomicBoolean isInitialProcessingComplete = new AtomicBoolean(false);

        String databaseId = cfg.getDatabaseId();
        String serviceEndpoint = cfg.getServiceEndpoint();
        String feedContainerId = UUID.randomUUID() + "-" + "feed";
        String leaseContainerId = UUID.randomUUID() + "-" + "lease";
        String owner = "HOST_0";
        String leasePrefix = "TEST";
        int maxItemCount = 1000;

        CosmosAsyncDatabase ffcfDatabase = null;
        CosmosAsyncContainer feedContainer = null, leaseContainer = null;
        final AtomicReference<BatchRunner> bulkIngestionAndSplitRunner = new AtomicReference<>();

        try (CosmosAsyncClient asyncClient = buildCosmosAsyncClient(cfg)) {

            ffcfDatabase = createDatabaseIfNotExists(asyncClient, databaseResourceId, databaseId);

            logger.info("Building feed container and lease container...");

            feedContainer = createContainerIfNotExists(ffcfDatabase, feedCollectionResourceId, feedContainerId, cfg.getFeedContainerInitialThroughput(), "/mypk");
            leaseContainer = createContainerIfNotExists(ffcfDatabase, leaseCollectionResourceId, leaseContainerId, 400, "/id");

            final LeaseManager leaseManager = new LeaseManager(
                leaseContainer,
                leasePrefix,
                cfg.getServiceEndpoint(),
                feedCollectionResourceId.get(),
                databaseResourceId.get());

            ChangeFeedProcessor changeFeedProcessor = buildChangeFeedProcessor(
                owner,
                feedContainer,
                leaseContainer,
                leaseManager,
                leasePrefix,
                cfpStartTime,
                cfpEndTime,
                idsFetched,
                isInitialProcessingComplete,
                docCountToRead,
                maxItemCount);


            bulkIngestionAndSplitRunner.set(new BatchRunner());

            Mono
                .just(changeFeedProcessor)
                .flatMap(ChangeFeedProcessor::start)
                .doOnSuccess(unused -> {
                        bulkIngestionExecutorService.submit(
                            () -> bulkIngestionAndSplitRunner.get().execute(cfg, feedContainerId));
                        leaseManager.takeLeaseSnapshot();
                    }
                )
                .block();

            // todo (abhmohanty): replace with latch / barrier wait
            while (!isInitialProcessingComplete.get()) {
                // do nothing
            }

            logger.info("Initial change feed processing complete.");
            changeFeedProcessor
                .stop()
                .doOnSuccess(unused -> logger.info("CFP stopped temporarily!"))
                .block();

            Thread.sleep(10_000);

            if (cfg.shouldResetLeaseContainer() && isInitialProcessingComplete.get()) {
                logger.info("Attempting to reset lease container...");
                idsFetched.clear();
                isInitialProcessingComplete.set(false);
                leaseManager.resetLeaseContainerToFullRangeLease();
                changeFeedProcessor
                    .start()
                    .doOnSuccess(unused -> logger.info("CFP started!"))
                    .block();
                Thread.sleep(30_000);
            }

            while (!isInitialProcessingComplete.get()) {
                // do nothing
            }

            if (isInitialProcessingComplete.get()) {
                Mono
                    .just(changeFeedProcessor)
                    .flatMap(ChangeFeedProcessor::stop)
                    .doOnSuccess(unused -> bulkIngestionExecutorService.shutdown())
                    .block();
            }

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
        LeaseManager leaseManager,
        String leasePrefix,
        final AtomicReference<Instant> startTime,
        final AtomicReference<Instant> endTime,
        final Set<String> idsFetched,
        final AtomicBoolean isProcessingComplete,
        final int idCountsToFetch,
        final int maxItemCount) {

        return new ChangeFeedProcessorBuilder()
            .hostName(hostName)
            .feedContainer(feedContainer)
            .leaseContainer(leaseContainer)
            .handleAllVersionsAndDeletesChanges((docs, context) -> {

                if (isProcessingComplete.get()) {
                    return;
                }

                if (idsFetched.isEmpty()) {
                    startTime.set(Instant.now());
                }

                if (idsFetched.size() >= maxItemCount && idsFetched.size() <= 2 * maxItemCount) {
                    leaseManager.takeLeaseSnapshot();
                }

                for (com.azure.cosmos.models.ChangeFeedProcessorItem doc : docs) {
                    idsFetched.add(doc.getCurrent().get("id").asText());
                }

                logger.info("Ids fetched : {}", idsFetched.size());

                if (idsFetched.size() >= idCountsToFetch) {
                    endTime.set(Instant.now());
                    isProcessingComplete.set(true);
                }

                logger.info("Lease token : {}", context.getLeaseToken());
            })
            .options(new ChangeFeedProcessorOptions()
                .setLeasePrefix(leasePrefix)
                .setStartFromBeginning(false)
                .setMaxItemCount(maxItemCount)
            )
            .buildChangeFeedProcessor();
    }

    private static CosmosAsyncDatabase createDatabaseIfNotExists(
        CosmosAsyncClient asyncClient,
        AtomicReference<String> databaseResourceId,
        String databaseId) {

        asyncClient
            .createDatabaseIfNotExists(databaseId)
            .doOnSuccess(cosmosDatabaseResponse -> databaseResourceId.set(cosmosDatabaseResponse.getProperties().getResourceId()))
            .block();

        return asyncClient.getDatabase(databaseId);
    }

    private static CosmosAsyncContainer createContainerIfNotExists(
        CosmosAsyncDatabase asyncDatabase,
        AtomicReference<String> collectionResourceId,
        String containerId,
        int throughput,
        String pkPath) {

        CosmosContainerProperties containerProperties = new CosmosContainerProperties(containerId, pkPath);
        asyncDatabase
            .createContainerIfNotExists(containerProperties, ThroughputProperties.createManualThroughput(throughput))
            .doOnSuccess(cosmosContainerResponse -> collectionResourceId.set(cosmosContainerResponse.getProperties().getResourceId()))
            .block();

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

    private static void isProcessingComplete() {

    }
}
