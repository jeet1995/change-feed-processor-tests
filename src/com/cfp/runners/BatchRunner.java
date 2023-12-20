package com.cfp.runners;

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.implementation.TestConfigurations;
import com.azure.cosmos.models.CosmosBulkOperations;
import com.azure.cosmos.models.CosmosItemOperation;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.models.ThroughputResponse;
import com.cfp.runners.entity.InternalObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class BatchRunner {

    private static final Logger logger = LoggerFactory.getLogger(BatchRunner.class);

    private final AtomicReference<Instant> splitStartTime = new AtomicReference<>();
    private final AtomicReference<Instant> splitEndTime = new AtomicReference<>();
    private final AtomicReference<Instant> ingestionStartTime = new AtomicReference<>();
    private final AtomicReference<Instant> ingestionEndTime = new AtomicReference<>();

    public void execute(Configuration cfg, String feedContainerId) {
        logger.info("Bulk ingestor started...");

        CosmosAsyncContainer feedContainer = null;

        try (CosmosAsyncClient asyncClient = buildCosmosAsyncClient()) {

            feedContainer = getCosmosAsyncContainer(
                asyncClient, cfg.getDatabaseId(), feedContainerId);

            logger.info("Ingest first batch of {} documents", cfg.getDocCountToIngestBeforeSplit());

            ingestionStartTime.set(Instant.now());

            // in case shouldFeedContainerSplit is set to false
            splitStartTime.set(Instant.now());
            splitEndTime.set(Instant.now());

            ingestDocuments(feedContainer, cfg.getDocCountToIngestBeforeSplit(), this.ingestionEndTime);

            if (cfg.shouldFeedContainerSplit()) {

                splitStartTime.set(Instant.now());
                logger.info("Split started!");

                ThroughputResponse throughputResponse = feedContainer.replaceThroughput(
                    ThroughputProperties.createManualThroughput(cfg.getFeedContainerNewProvisionedThroughput())).block();

                while (true) {
                    assert throughputResponse != null;

                    throughputResponse = feedContainer.readThroughput().block();

                    if (!throughputResponse.isReplacePending()) {
                        logger.info("Split completed!");
                        splitEndTime.set(Instant.now());
                        break;
                    }
                }
            }

            logger.info("Ingest second batch of {} documents", cfg.getDocCountToIngestAfterSplit());
            ingestDocuments(feedContainer, cfg.getDocCountToIngestAfterSplit(), this.ingestionEndTime);

        } catch (Exception ex) {
            logger.error("Exception caught : {}", ex.toString());
        }
    }

    private static CosmosAsyncClient buildCosmosAsyncClient() {

        CosmosAsyncClient client = new CosmosClientBuilder()
            .endpoint(TestConfigurations.HOST)
            .key(TestConfigurations.MASTER_KEY)
            .contentResponseOnWriteEnabled(true)
            .buildAsyncClient();

        return client;
    }

    private static CosmosAsyncContainer getCosmosAsyncContainer(CosmosAsyncClient client, String databaseId, String containerId) {
        return client.getDatabase(databaseId).getContainer(containerId);
    }

    private static void ingestDocuments(CosmosAsyncContainer asyncContainer, int docCount, AtomicReference<Instant> ingestionEndTime) {

        List<InternalObject> objectsToUpsert = new ArrayList<>();

        for (int i = 1; i <= docCount; i++) {

            StringBuilder sb = new StringBuilder();

            for (int j = 1; j <= 1; j++) {
                sb.append("X");
            }

            InternalObject internalObject = InternalObject.createInternalObject(sb.toString());

            objectsToUpsert.add(internalObject);
        }

        Flux<InternalObject> internalObjectFlux = Flux.fromIterable(objectsToUpsert);

        Flux<CosmosItemOperation> cosmosItemOperations = internalObjectFlux
            .map(internalObject -> CosmosBulkOperations.getUpsertItemOperation(internalObject, new PartitionKey(internalObject.getMypk())));

        asyncContainer
            .executeBulkOperations(cosmosItemOperations)
            .flatMap(bulkOperationResponse -> {
                //                logger.info("Response : {}", bulkOperationResponse.getResponse().getStatusCode());
                return Mono.just(bulkOperationResponse);
            })
            .doOnComplete(() -> {
                ingestionEndTime.set(Instant.now());
                logger.info("Ingestion of {} documents complete!", docCount);
            })
            .blockLast();
    }

    public AtomicReference<Instant> getSplitStartTime() {
        return splitStartTime;
    }

    public AtomicReference<Instant> getSplitEndTime() {
        return splitEndTime;
    }

    public AtomicReference<Instant> getIngestionStartTime() {
        return ingestionStartTime;
    }

    public AtomicReference<Instant> getIngestionEndTime() {
        return ingestionEndTime;
    }
}
