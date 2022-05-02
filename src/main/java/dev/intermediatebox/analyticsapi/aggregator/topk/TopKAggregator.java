package dev.intermediatebox.analyticsapi.aggregator.topk;

import java.util.List;
import java.util.Map;

public interface TopKAggregator<T> {
  void insertItem(T item);

  List<Map<T, Integer>> getTopK(int k);

  int getProcessedItemsCount();
}
