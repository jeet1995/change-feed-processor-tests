package test.ingestors;

import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import test.entity.InternalObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class PointCreateIngestor implements Ingestor {

    private static final Logger logger = LoggerFactory.getLogger(PointCreateIngestor.class);

    @Override
    public void ingestDocuments(CosmosAsyncContainer container, int expectedInsertions) {

        AtomicInteger docCountSuccessfullyInserted = new AtomicInteger(0);

        Flux.range(0, expectedInsertions)
                .flatMap(integer -> {
                    InternalObject internalObject = InternalObject.createInternalObject(UUID.randomUUID().toString());
                    return container.upsertItem(
                                    internalObject,
                                    new PartitionKey(internalObject.getMypk()),
                                    new CosmosItemRequestOptions())
                            .doOnSuccess(response -> docCountSuccessfullyInserted.incrementAndGet())
                            .onErrorResume(throwable -> {
                                logger.warn("Throwable: ", throwable);
                                return Mono.empty();
                            });
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
