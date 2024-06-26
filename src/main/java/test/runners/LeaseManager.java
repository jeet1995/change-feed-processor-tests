package test.runners;

import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.implementation.HttpConstants;
import com.azure.cosmos.implementation.feedranges.FeedRangeEpkImpl;
import com.azure.cosmos.models.PartitionKey;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class LeaseManager {

    private static final Logger logger = LoggerFactory.getLogger(LeaseManager.class);
    private static final FeedRangeEpkImpl fullFeedRange = FeedRangeEpkImpl.forFullRange();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final AtomicReference<JsonNode> lastLeaseSnapshot = new AtomicReference<>();
    private final CosmosAsyncContainer leaseContainer;

    public LeaseManager(CosmosAsyncContainer leaseContainer) {
        this.leaseContainer = leaseContainer;
    }

    public synchronized void resetLeaseContainerToFullRangeLease() throws JsonProcessingException {

        String leaseQuery = "select * from c where not contains(c.id, \"info\")";

        List<JsonNode> leaseDocuments = leaseContainer
            .queryItems(leaseQuery, JsonNode.class)
            .collectList()
            .block();

        if (leaseDocuments == null || leaseDocuments.isEmpty()) {
            logger.warn("No lease documents found");
            return;
        }

        // delete leases in the lease container
        for (JsonNode leaseDocument : leaseDocuments) {

            String leaseId = leaseDocument.get("id").asText();

            leaseContainer
                .deleteItem(leaseId, new PartitionKey(leaseId))
                .doOnSuccess(response -> logger.info("Lease with id : {} has been deleted successfully.", leaseId))
                .block();
        }

        JsonNode lastRecordedLease = lastLeaseSnapshot.get();

        leaseContainer
                .createItem(lastRecordedLease)
                .doOnSuccess(response -> {
                    if (response.getStatusCode() == HttpConstants.StatusCodes.CREATED) {
                        logger.info(
                                "Lease item with id : {} successfully created manually.",
                                response.getItem().get("id").asText());
                    }
                })
                .block();
    }

    public synchronized boolean takeLeaseSnapshot(String feedContainerRid) {
        String leaseQuery = "select * from c where not contains(c.id, \"info\")";

        List<JsonNode> leaseDocuments = leaseContainer
            .queryItems(leaseQuery, JsonNode.class)
            .collectList()
            .block();

        if (leaseDocuments == null || leaseDocuments.isEmpty()) {
            logger.warn("No lease documents found");
            return false;
        }

        // take the first lease document in the list of lease documents
        JsonNode leaseDocument = leaseDocuments.get(0);
        String continuationWithZeroLsn = decorateContinuationWithFeedCollectionRid(feedContainerRid, loadTemplateContinuationForFullFeedRange());
        String encodedContinuationWithZeroLsn =
                Base64.getEncoder().encodeToString(continuationWithZeroLsn.getBytes(StandardCharsets.UTF_8));

        ((ObjectNode) leaseDocument).put("ContinuationToken", encodedContinuationWithZeroLsn);

        this.lastLeaseSnapshot.set(leaseDocument);
        return true;
    }

    private static String decorateContinuationWithFeedCollectionRid(String feedCollectionRid, String changeFeedContinuation) {
        JsonNode changeFeedJsonNode = null;
        try {
            changeFeedJsonNode = OBJECT_MAPPER.readTree(changeFeedContinuation);
            ((ObjectNode) changeFeedJsonNode).put("Rid", feedCollectionRid);

            JsonNode continuationAsJsonNode = changeFeedJsonNode.get("Continuation");
            ((ObjectNode) continuationAsJsonNode).put("Rid", feedCollectionRid);

            return OBJECT_MAPPER.writeValueAsString(changeFeedJsonNode);
        } catch (JsonProcessingException e) {
            logger.error("Continuation - \n {} \n could not be de-serialized as JsonNode!", changeFeedContinuation);
            return null;
        }
    }

    private static String loadTemplateContinuationForFullFeedRange() {
        return "{\n" +
                "    \"V\": 1,\n" +
                "    \"Rid\": \"\",\n" +
                "    \"Mode\": \"FULL_FIDELITY\",\n" +
                "    \"StartFrom\": {\n" +
                "        \"Type\": \"NOW\"\n" +
                "    },\n" +
                "    \"Continuation\": {\n" +
                "        \"V\": 1,\n" +
                "        \"Rid\": \"\",\n" +
                "        \"Continuation\": [\n" +
                "            {\n" +
                "                \"token\": \"\\\"0\\\"\",\n" +
                "                \"range\": {\n" +
                "                    \"min\": \"\",\n" +
                "                    \"max\": \"FF\"\n" +
                "                }\n" +
                "            }\n" +
                "        ],\n" +
                "        \"Range\": {\n" +
                "            \"min\": \"\",\n" +
                "            \"max\": \"FF\",\n" +
                "            \"isMinInclusive\": true,\n" +
                "            \"isMaxInclusive\": false\n" +
                "        }\n" +
                "    }\n" +
                "}";
    }
}
