package dev.intermediatebox.analyticsapi.aggregator.topk;

import dev.intermediatebox.analyticsapi.aggregator.topk.spaceSaving.StreamSummary;
import dev.intermediatebox.analyticsapi.application.SpringContext;
import dev.intermediatebox.analyticsapi.query.QueryMapper;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

@Slf4j
public class SpaceSavingTopKAggregator<T> implements TopKAggregator<T> {
  private StreamSummary streamSummary;

  private QueryMapper queryMapper;

  private int processedItemsCount = 0;

  public SpaceSavingTopKAggregator() {
    double spaceSavingError = Double.valueOf(SpringContext.getProperty("algorithm.topk.spaceSaving.error"));
    this.streamSummary = new StreamSummary(spaceSavingError);
    this.queryMapper = new QueryMapper();
  }

  @Override
  public synchronized void insertItem(T item) {
    this.streamSummary.insert(item);

    processedItemsCount++;
    if (processedItemsCount % 10000 == 0) {
      log.info(String.format("%s total processed items.", processedItemsCount));
    }
  }

  @Override
  public List<Map<T, Integer>> getTopK(int k) {
    Instant startTime = Instant.now();
    List<Map<T, Integer>> topK = queryMapper.toQueryTopKServiceEntity(this.streamSummary.getTopK(k));
    Instant endTime = Instant.now();
    log.info(String.format("Computing top k took %sms to complete.", Duration.between(startTime, endTime).toMillis()));

    return topK;
  }

  @Override
  public int getProcessedItemsCount() {
    return processedItemsCount;
  }
}
