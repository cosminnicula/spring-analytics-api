package dev.intermediatebox.analyticsapi.aggregator.topk;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@NoArgsConstructor
@Slf4j
// O(k) space complexity, where k < n, and n == number of distinct items. not suitable for large k.
public class NaiveTopKAggregator<T> implements TopKAggregator<T> {
  // if there are more than Integer.MAX_VALUE distinct items, Map<> should be replaced with something like
  // bucketing Map<Map<>> (Map has max size == Integer.MAX_VALUE).
  // if at least one item occurs more than Integer.MAX_VALUE times, then frequencies needs to be stored
  // in something like a singly linked list (List has max size == Integer.MAX_VALUE), kept sorted in decreasing order.
  // anyway, addressing either of these two cases would ultimately lead to high memory usage which does not scale.
  private Map<T, Integer> items = new HashMap<>();

  private int processedItemsCount = 0;

  @Override
  public synchronized void insertItem(T item) {
    var frequency = items.containsKey(item) ? items.get(item) + 1 : 1;
    items.put(item, frequency);

    processedItemsCount++;
    if (processedItemsCount % 10000 == 0) {
      log.info(String.format("%s total processed items.", processedItemsCount));
    }
  }

  @Override
  public List<Map<T, Integer>> getTopK(int k) {
    if (k <= 0) {
      throw new IllegalArgumentException("k should be > 0");
    }

    Instant startTime = Instant.now();

    // List<List<>> is needed for the edge case where at least two items have the same count.
    List<List<T>> frequencies = new ArrayList<>();

    for (int i = 0; i <= items.size(); i++) {
      frequencies.add(new ArrayList<>());
    }

    for (Map.Entry<T, Integer> item : items.entrySet()) {
      // key == item, value == frequency
      frequencies.get(item.getValue()).add(item.getKey());
    }

    int kCounter = 0;
    boolean hasTopK = false;
    List<Map<T, Integer>> topK = new ArrayList<>();
    // item index == frequency => highest index == highest frequency => iterate items in reverse order (from
    // highest to lowest index) to get frequencies in descending order.
    for (int i = items.size(); i >= 0 && !hasTopK; i--) {
      for (T item : frequencies.get(i)) {
        // i is the item's frequency
        topK.add(Map.of(item, i));
        kCounter++;
        if (kCounter == k) {
          hasTopK = true;
          break;
        }
      }
    }

    Instant endTime = Instant.now();
    log.info(String.format("Computing top k took %sms to complete.", Duration.between(startTime, endTime).toMillis()));

    return topK;
  }

  @Override
  public int getProcessedItemsCount() {
    return processedItemsCount;
  }
}