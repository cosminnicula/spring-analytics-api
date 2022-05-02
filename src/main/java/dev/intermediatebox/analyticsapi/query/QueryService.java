package dev.intermediatebox.analyticsapi.query;

import dev.intermediatebox.analyticsapi.aggregator.distinct.NaiveDistinctAggregator;
import dev.intermediatebox.analyticsapi.aggregator.topk.SpaceSavingTopKAggregator;
import dev.intermediatebox.analyticsapi.dataset.hn.DatasetService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class QueryService {
  private final DatasetService datasetService;

  public long getDistinctCount(LocalDateTime startDate, LocalDateTime endDate) {
    var distinctAggregator = new NaiveDistinctAggregator<Long>();

    Instant startTime = Instant.now();
    datasetService.getData(startDate, endDate).stream().parallel().forEach(queryEntity -> {
      distinctAggregator.insertItem(queryEntity.getQueryHash());
    });
    Instant endTime = Instant.now();
    long durationMillis = Duration.between(startTime, endTime).toMillis();
    log.info(String.format("Processing distinct count took %sms to complete.", durationMillis));
    log.info(String.format("Distinct count throughput = %s items/s.", distinctAggregator.getProcessedItemsCount() / durationMillis * 1000));

    return distinctAggregator.getDistinctCount();
  }

  public List<Map<String, Integer>> getTopKQueries(LocalDateTime startDate, LocalDateTime endDate, Integer k) {
    var topKAggregator = new SpaceSavingTopKAggregator<String>();

    Instant startTime = Instant.now();
    datasetService.getData(startDate, endDate).stream().parallel().forEach(queryEntity -> {
      topKAggregator.insertItem(queryEntity.getQuery());
    });
    Instant endTime = Instant.now();
    long durationMillis = Duration.between(startTime, endTime).toMillis();
    log.info(String.format("Processing top k took %sms to complete.", durationMillis));
    log.info(String.format("Top k throughput = %s items/s.", topKAggregator.getProcessedItemsCount() / durationMillis * 1000));

    return topKAggregator.getTopK(k);
  }
}
