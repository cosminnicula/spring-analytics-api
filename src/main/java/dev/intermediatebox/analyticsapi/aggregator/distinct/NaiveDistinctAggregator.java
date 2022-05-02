package dev.intermediatebox.analyticsapi.aggregator.distinct;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

@NoArgsConstructor
@Slf4j
public class NaiveDistinctAggregator<T> implements DistinctAggregator<T> {
  private Set<T> items = new HashSet<>();

  private AtomicInteger atomicProcessedItemsCount = new AtomicInteger(0);

  @Override
  public synchronized void insertItem(T item) {
    items.add(item);

    int processedItemsCount = atomicProcessedItemsCount.incrementAndGet();
    if (processedItemsCount % 10000 == 0) {
      log.info(String.format("%s total processed items.", processedItemsCount));
    }
  }

  @Override
  public long getDistinctCount() {
    Instant startTime = Instant.now();
    long count = items.stream().count();
    Instant endTime = Instant.now();
    log.info(String.format("Computing distinct count took %sms to complete.", Duration.between(startTime, endTime).toMillis()));

    return count;
  }

  @Override
  public int getProcessedItemsCount() {
    return items.size();
  }
}
