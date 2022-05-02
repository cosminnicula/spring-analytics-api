package dev.intermediatebox.analyticsapi.aggregator.distinct;

public interface DistinctAggregator<T> {
  void insertItem(T item);

  long getDistinctCount();

  int getProcessedItemsCount();
}
