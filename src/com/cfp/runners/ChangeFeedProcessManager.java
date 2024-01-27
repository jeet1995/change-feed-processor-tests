package com.cfp.runners;

import com.azure.cosmos.ChangeFeedProcessor;
import com.azure.cosmos.ChangeFeedProcessorBuilder;
import com.azure.cosmos.ChangeFeedProcessorContext;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.models.ChangeFeedProcessorItem;
import com.azure.cosmos.models.ChangeFeedProcessorOptions;
import com.azure.cosmos.models.CosmosChangeFeedRequestOptions;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.FeedResponse;
import com.azure.cosmos.models.ModelBridgeInternal;
import com.azure.cosmos.models.ThroughputProperties;
import com.cfp.runners.entity.RequestResponseEntity;
import com.cfp.runners.entity.TonedDownFeedResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class ChangeFeedProcessManager {

    private static final Logger logger = LoggerFactory.getLogger(ChangeFeedProcessManager.class);

    private static final ExecutorService bulkIngestionExecutorService = new ScheduledThreadPoolExecutor(1);

    public void execute(Configuration cfg) {

        int docCountToRead = cfg.getDocCountToIngestBeforeSplit() + cfg.getDocCountToIngestAfterSplit();
        String runId = UUID.randomUUID().toString();

        AtomicReference<Instant> cfpStartTime = new AtomicReference<>();
        AtomicReference<Instant> cfpEndTime = new AtomicReference<>();

        // todo (abhmohanty): replace with value holder
        AtomicReference<String> databaseResourceId = new AtomicReference<>();
        AtomicReference<String> feedCollectionResourceId = new AtomicReference<>();
        AtomicReference<String> leaseCollectionResourceId = new AtomicReference<>();
        AtomicReference<ChangeFeedExecutionContext> changeFeedExecutionContextRef = new AtomicReference<>(new ChangeFeedExecutionContext(docCountToRead, false));

        AtomicBoolean isInitialProcessingComplete = new AtomicBoolean(false);

        String databaseId = cfg.getDatabaseId();
        String feedContainerId = cfg.getFeedContainerId().isEmpty() ? runId + "-" + "feed" : cfg.getFeedContainerId();
        String leaseContainerId = cfg.getLeaseContainerId().isEmpty() ? runId + "-" + "lease" : cfg.getLeaseContainerId();
        String owner = "HOST_0";
        String leasePrefix = "TEST";

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

            final Supplier<ChangeFeedExecutionContext> changeFeedExecutionContextSupplier = () -> changeFeedExecutionContextRef.get();

            ChangeFeedProcessor changeFeedProcessor = buildChangeFeedProcessor(
                    owner,
                    feedContainer,
                    leaseContainer,
                    leaseManager,
                    leasePrefix,
                    cfpStartTime,
                    cfpEndTime,
                    changeFeedExecutionContextSupplier,
                    isInitialProcessingComplete);


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
                isInitialProcessingComplete.set(false);
                leaseManager.resetLeaseContainerToFullRangeLease();
                FileUtils.writeRequestResponseEntitiesToFile(changeFeedExecutionContextSupplier.get().getRequestResponseEntities(), runId + "_" + "req_res_timeline_before_lease_reset.json");
                changeFeedExecutionContextRef.set(new ChangeFeedExecutionContext(docCountToRead, true));

                changeFeedProcessor
                    .start()
                    .doOnSuccess(unused -> {
                        logger.info("CFP started!");

                    })
                    .block();
                Thread.sleep(30_000);
            }

            while (!isInitialProcessingComplete.get() && !isJobHangDetected(changeFeedExecutionContextSupplier.get().getLastProcessedCfpBatchInstant().get())) {
                // do nothing
            }

            if (isInitialProcessingComplete.get() || isJobHangDetected(changeFeedExecutionContextSupplier.get().getLastProcessedCfpBatchInstant().get())) {
                FileUtils.writeRequestResponseEntitiesToFile(changeFeedExecutionContextSupplier.get().getRequestResponseEntities(), runId + "_" + "req_res_timeline_after_lease_reset.json");
                if (changeFeedProcessor.isStarted()) {
                    Mono.just(changeFeedProcessor).flatMap(ChangeFeedProcessor::stop).block();
                }

                bulkIngestionExecutorService.shutdown();
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
            final Supplier<ChangeFeedExecutionContext> changeFeedExecutionContextSupplier,
            final AtomicBoolean isChangeFeedProcessingComplete) {

        return new ChangeFeedProcessorBuilder()
            .hostName(hostName)
            .feedContainer(feedContainer)
            .leaseContainer(leaseContainer)
            .handleAllVersionsAndDeletesChanges((docs, context) -> {

                List<String> docIdsProcessedInChangeFeedBatch = new ArrayList<>();
                ChangeFeedExecutionContext changeFeedExecutionContext = changeFeedExecutionContextSupplier.get();

                if (changeFeedExecutionContext.getIsProcessingComplete().get()) {
                    return;
                }

                Set<String> idsFetched = changeFeedExecutionContext.getIdsFetched();

                if (idsFetched.isEmpty()) {
                    startTime.set(Instant.now());
                }

                AtomicBoolean isFirstChangeFeedBatch = changeFeedExecutionContext.getIsFirstChangeFeedBatch();
                AtomicBoolean isChangeFeedReprocessing = changeFeedExecutionContext.getIsChangeFeedReprocessing();

                AtomicInteger batchCount = changeFeedExecutionContext.getBatchCount();

                if (batchCount.incrementAndGet() == 1 && !isChangeFeedReprocessing.get()) {
                    boolean isSuccessfulSnapshot = leaseManager.takeLeaseSnapshot();

                    if (isSuccessfulSnapshot) {
                        isFirstChangeFeedBatch.set(false);
                    }
                }

                for (com.azure.cosmos.models.ChangeFeedProcessorItem doc : docs) {

                    String docId = doc.getCurrent().get("id").asText();

                    idsFetched.add(docId);
                    docIdsProcessedInChangeFeedBatch.add(docId);
                }

                int idCountsToFetch = changeFeedExecutionContext.getIdCountsToFetch();

                logger.info("Ids fetched : {}", idsFetched.size());

                if (idsFetched.size() >= idCountsToFetch) {
                    endTime.set(Instant.now());
                    isChangeFeedProcessingComplete.set(true);
                }

                AtomicReference<Instant> lastProcessedCfpBatchInstant = changeFeedExecutionContext.getLastProcessedCfpBatchInstant();

                lastProcessedCfpBatchInstant.set(Instant.now());
                addRequestResponseEntityToList(context, docIdsProcessedInChangeFeedBatch, changeFeedExecutionContext.getRequestResponseEntities(), isChangeFeedReprocessing);
            })
            .options(new ChangeFeedProcessorOptions()
                .setLeasePrefix(leasePrefix)
                .setStartFromBeginning(false)
                .setMaxItemCount(1)
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

    private static void isProcessingComplete() {}

    private static synchronized void addRequestResponseEntityToList(
            ChangeFeedProcessorContext<ChangeFeedProcessorItem> context,
            List<String> docIdsFromChangeFeedBatch,
            List<RequestResponseEntity> requestResponseEntities,
            AtomicBoolean isReprocessing) {

        RequestResponseEntity requestResponseEntity = new RequestResponseEntity();

        FeedResponse<ChangeFeedProcessorItem> feedResponse = context.getFeedResponse();
        CosmosChangeFeedRequestOptions changeFeedRequestOptions = context.getCosmosChangeFeedRequestOptions();
        String leaseToken = context.getLeaseToken();

        TonedDownFeedResponse tonedDownFeedResponse = new TonedDownFeedResponse();
        tonedDownFeedResponse.setHeader(feedResponse.getResponseHeaders());
        tonedDownFeedResponse.setDiagnosticString(feedResponse.getCosmosDiagnostics().toString());

        requestResponseEntity.setLeaseToken(leaseToken);
        requestResponseEntity.setTonedDownFeedResponse(tonedDownFeedResponse);
        requestResponseEntity.setResponseHeaders(feedResponse.getResponseHeaders());
        requestResponseEntity.setContinuationStateFromRequest(new String(Base64.getUrlDecoder().decode(ModelBridgeInternal.getChangeFeedContinuationState(changeFeedRequestOptions).toString())));
        requestResponseEntity.setContinuationStateFromResponse(new String(Base64.getUrlDecoder().decode(feedResponse.getContinuationToken())));
        requestResponseEntity.setDocIdsFromChangeFeedBatch(docIdsFromChangeFeedBatch);
        requestResponseEntity.setChangeFeedBeingReprocessed(isReprocessing.get());

        requestResponseEntities.add(requestResponseEntity);
    }

    public static boolean isJobHangDetected(Instant lastProcessedInstantByCfp) {
        return Duration.between(lastProcessedInstantByCfp, Instant.now()).compareTo(Duration.ofSeconds(60)) > 0;
    }
}
