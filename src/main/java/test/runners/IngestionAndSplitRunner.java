package test.runners;

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.models.ThroughputResponse;
import test.Configuration;
import test.ingestors.Ingestor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestionAndSplitRunner {

    private static final Logger logger = LoggerFactory.getLogger(IngestionAndSplitRunner.class);

    private final Ingestor ingestor;

    public IngestionAndSplitRunner(Ingestor ingestor) {
        this.ingestor = ingestor;
    }

    public void execute(Configuration cfg, String feedContainerId) {
        logger.info("Bulk ingestor started...");

        CosmosAsyncContainer feedContainer = null;

        try (CosmosAsyncClient asyncClient = buildCosmosAsyncClient(cfg)) {

            feedContainer = getCosmosAsyncContainer(
                asyncClient, cfg.getDatabaseId(), feedContainerId);

            logger.info("Ingest first batch of {} documents", cfg.getDocCountToIngestBeforeSplit());


            ingestDocuments(feedContainer, cfg.getDocCountToIngestBeforeSplit());

            if (cfg.shouldFeedContainerSplit()) {
                logger.info("Split started!");

                ThroughputResponse throughputResponse = feedContainer.replaceThroughput(
                    ThroughputProperties.createManualThroughput(cfg.getFeedContainerNewProvisionedThroughput())).block();

                while (true) {
                    assert throughputResponse != null;

                    throughputResponse = feedContainer.readThroughput().block();

                    if (!throughputResponse.isReplacePending()) {
                        logger.info("Split completed!");
                        break;
                    }
                }
            }

            logger.info("Ingest second batch of {} documents", cfg.getDocCountToIngestAfterSplit());
            ingestDocuments(feedContainer, cfg.getDocCountToIngestAfterSplit());

        } catch (Exception ex) {
            logger.error("Exception caught : {}", ex.toString());
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

    private static CosmosAsyncContainer getCosmosAsyncContainer(CosmosAsyncClient client, String databaseId, String containerId) {
        return client.getDatabase(databaseId).getContainer(containerId);
    }

    private void ingestDocuments(CosmosAsyncContainer container, int docCount) {
        this.ingestor.ingestDocuments(container, docCount);
    }
}
