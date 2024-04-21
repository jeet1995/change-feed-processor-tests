package com.cfp.test.ingestors;

import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.implementation.HttpConstants;
import com.azure.cosmos.models.CosmosBulkExecutionOptions;
import com.azure.cosmos.models.CosmosBulkItemResponse;
import com.azure.cosmos.models.CosmosBulkOperations;
import com.azure.cosmos.models.CosmosItemOperation;
import com.azure.cosmos.models.PartitionKey;
import com.cfp.test.Configuration;
import com.cfp.test.entity.InternalObject;
import com.cfp.test.ingestors.Ingestor;
import com.cfp.test.ingestors.PointCreateIngestor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class BulkIngestor implements Ingestor {

    private static final Logger logger = LoggerFactory.getLogger(PointCreateIngestor.class);
    private final Configuration cfg;

    public BulkIngestor(Configuration cfg) {
        this.cfg = cfg;
    }

    @Override
    public void ingestDocuments(CosmosAsyncContainer container, int expectedInsertions) {
        CosmosBulkExecutionOptions bulkExecutionOptions = new CosmosBulkExecutionOptions();

        bulkExecutionOptions.setInitialMicroBatchSize(cfg.getBulkIngestionMicroBatchSize());

        AtomicInteger docCountSuccessfullyInserted = new AtomicInteger();

        // test will cause OOM errors with high counts
        List<InternalObject> objectsToUpsert = new ArrayList<>();

        for (int i = 1; i <= expectedInsertions; i++) {
            InternalObject internalObject = InternalObject.createInternalObject(UUID.randomUUID().toString());
            objectsToUpsert.add(internalObject);
        }

        Flux<InternalObject> internalObjectFlux = Flux.fromIterable(objectsToUpsert);

        Flux<CosmosItemOperation> cosmosItemOperations = internalObjectFlux
            .map(internalObject -> CosmosBulkOperations.getUpsertItemOperation(internalObject, new PartitionKey(internalObject.getMypk())));

        container
            .executeBulkOperations(cosmosItemOperations, bulkExecutionOptions)
            .flatMap(bulkOperationResponse -> {
                CosmosBulkItemResponse bulkItemResponse = bulkOperationResponse.getResponse();

                if (bulkItemResponse.getStatusCode() == HttpConstants.StatusCodes.CREATED) {
                    docCountSuccessfullyInserted.incrementAndGet();
                }

                return Mono.just(bulkOperationResponse);
            })
            .onErrorResume(throwable -> {
                logger.warn("Throwable: {}", throwable.toString());
                return Mono.empty();
            })
            .doOnComplete(() -> {

                if (docCountSuccessfullyInserted.get() < expectedInsertions) {
                    logger.warn("Expected insertion count : {}; Actual insertion count : {}", expectedInsertions, docCountSuccessfullyInserted.get());
                } else {
                    logger.info("Ingestion of {} documents complete!", expectedInsertions);
                }
            })
            .blockLast();
    }
}
