package com.cfp.runners.entity;

import com.azure.cosmos.models.ChangeFeedProcessorItem;
import com.azure.cosmos.models.FeedResponse;

import java.util.List;
import java.util.Map;

public class RequestResponseEntity {

    private String continuationStateFromRequest;
    private String continuationStateFromResponse;
    private Map<String, String> responseHeaders;
//    private List<String> docIdsFromChangeFeedBatch;
    private String leaseToken;
    private TonedDownFeedResponse items;
    private boolean isChangeFeedBeingReprocessed;

    public RequestResponseEntity() {}

    public void setContinuationStateFromRequest(String continuationStateFromRequest) {
        this.continuationStateFromRequest = continuationStateFromRequest;
    }

    public void setContinuationStateFromResponse(String continuationStateFromResponse) {
        this.continuationStateFromResponse = continuationStateFromResponse;
    }

    public void setResponseHeaders(Map<String, String> responseHeaders) {
        this.responseHeaders = responseHeaders;
    }

//    public void setDocIdsFromChangeFeedBatch(List<String> docIdsFromChangeFeedBatch) {
//        this.docIdsFromChangeFeedBatch = docIdsFromChangeFeedBatch;
//    }

    public void setLeaseToken(String leaseToken) {
        this.leaseToken = leaseToken;
    }

    public void setTonedDownFeedResponse(TonedDownFeedResponse items) {
        this.items = items;
    }

    public void setChangeFeedBeingReprocessed(boolean changeFeedBeingReprocessed) {
        isChangeFeedBeingReprocessed = changeFeedBeingReprocessed;
    }

    public String getContinuationStateFromRequest() {
        return continuationStateFromRequest;
    }

    public String getContinuationStateFromResponse() {
        return continuationStateFromResponse;
    }

    public Map<String, String> getResponseHeaders() {
        return responseHeaders;
    }

//    public List<String> getDocIdsFromChangeFeedBatch() {
//        return docIdsFromChangeFeedBatch;
//    }

    public String getLeaseToken() {
        return leaseToken;
    }

    public TonedDownFeedResponse getItems() {
        return items;
    }

    public boolean isChangeFeedBeingReprocessed() {
        return isChangeFeedBeingReprocessed;
    }
}
