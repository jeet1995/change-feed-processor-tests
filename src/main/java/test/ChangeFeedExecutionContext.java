package test;

import test.entity.RequestResponseEntity;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ChangeFeedExecutionContext {

    private final Set<String> idsFetched;
    private final AtomicBoolean isProcessingComplete;
    private final int idCountsToFetch;
    private final AtomicBoolean isLeaseSnapshotTaken;
    private final AtomicBoolean isChangeFeedReprocessing;
    private final List<RequestResponseEntity> requestResponseEntities;
    private final AtomicReference<Instant> lastProcessedCfpBatchInstant;

    public AtomicInteger getBatchCount() {
        return batchCount;
    }

    private final AtomicInteger batchCount;

    public ChangeFeedExecutionContext(int idCountsToFetch, boolean isChangeFeedReprocessing) {
        this.idsFetched = ConcurrentHashMap.newKeySet();
        this.isProcessingComplete = new AtomicBoolean(false);
        this.idCountsToFetch = idCountsToFetch;
        this.isLeaseSnapshotTaken = new AtomicBoolean(false);
        this.isChangeFeedReprocessing = new AtomicBoolean(isChangeFeedReprocessing);
        this.requestResponseEntities = new ArrayList<>();
        this.lastProcessedCfpBatchInstant = new AtomicReference<>(Instant.now());
        this.batchCount = new AtomicInteger(0);
    }

    public Set<String> getIdsFetched() {
        return idsFetched;
    }

    public AtomicBoolean getIsProcessingComplete() {
        return isProcessingComplete;
    }

    public int getIdCountsToFetch() {
        return idCountsToFetch;
    }

    public AtomicBoolean getIsLeaseSnapshotTaken() {
        return isLeaseSnapshotTaken;
    }

    public AtomicBoolean getIsChangeFeedReprocessing() {
        return isChangeFeedReprocessing;
    }

    public List<RequestResponseEntity> getRequestResponseEntities() {
        return requestResponseEntities;
    }

    public AtomicReference<Instant> getLastProcessedCfpBatchInstant() {
        return lastProcessedCfpBatchInstant;
    }
}
