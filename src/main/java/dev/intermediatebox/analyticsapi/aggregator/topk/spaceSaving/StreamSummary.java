package dev.intermediatebox.analyticsapi.aggregator.topk.spaceSaving;

import dev.intermediatebox.analyticsapi.aggregator.dataStructure.DoubleLinkedList;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
@Setter
@ToString
// https://www.cse.ust.hk/~raywong/comp5331/References/EfficientComputationOfFrequentAndTop-kElementsInDataStreams.pdf
// 3.1 The Space-Saving Algorithm
public class StreamSummary<T> {

  // a StreamSummary has a list of Buckets.
  // each Bucket has a list of Counters; aforementioned Counters have the same frequency.
  // each Counter points to its parent Bucket.
  // Buckets are stored in double linked list in descending order.
  private DoubleLinkedList<Bucket<T>> buckets = new DoubleLinkedList<>();

  // items are stored in a hash table for constant amortized access cost.
  // O(counterSize) space complexity
  private Map<T, Counter<T>> items = new HashMap<>();

  // fixed Counters list size (size == 1/error)
  private int countersSize;

  // total number of processed items
  private long processedItemsCount;

  // initialize first Bucket with the list of Counters.
  public StreamSummary(double error) {
    if (error <= 0 || error > 1) {
      throw new IllegalArgumentException("Error should belong to interval (0,1]");
    }

    this.countersSize = (int) Math.ceil(1.0 / error);
    // initialize first Bucket with 0 frequency. the Bucket itself has a list of 0 frequency Counters.
    // each Counter's parent is the aforementioned Bucket.
    var firstBucket = new Bucket<T>(0L);
    var firstBucketNode = new DoubleLinkedList.Node<>(firstBucket);
    for (int i = 0; i < countersSize; i++) {
      firstBucket.getChildren().add(new Counter<T>(firstBucketNode));
    }

    buckets.insertFirst(firstBucketNode);
  }

  // Counters will move between buckets as more items are being processed.
  public void insert(T item) {
    processedItemsCount++;

    Counter<T> counter;
    if (items.containsKey(item)) {
      counter = items.get(item);
    } else {
      counter = buckets.getTail().getItem().getChildren().getFirst();
      updateItems(item, counter);
    }

    updateBuckets(counter);
  }

  public List<Counter<T>> getTopK(int k) {
    return buckets.stream().flatMap(b -> b.getChildren().stream())
        .limit(k).collect(Collectors.toList());
  }

  private void updateBuckets(Counter<T> counter) {
    var currentBucket = counter.getParent();
    var prevBucket = currentBucket.getPrev();

    counter.setFrequency(counter.getFrequency() + 1);

    currentBucket.getItem().getChildren().remove(counter);

    if (prevBucket != null && counter.getFrequency() == prevBucket.getItem().getFrequency()) {
      prevBucket.getItem().getChildren().addLast(counter);
      counter.setParent(prevBucket);
    } else {
      var newBucket = new DoubleLinkedList.Node<>(new Bucket<T>(counter.getFrequency()));
      newBucket.getItem().getChildren().addLast(counter);
      buckets.insertBefore(currentBucket, newBucket);
      counter.setParent(newBucket);
    }

    if (currentBucket.getItem().getChildren().isEmpty()) {
      buckets.remove(currentBucket);
    }
  }

  private void updateItems(T item, Counter<T> counter) {
    var frequency = counter.getFrequency();

    if (items.containsKey(counter.getItem())) {
      items.remove(counter.getItem());
    }

    counter.setItem(item);
    items.put(item, counter);

    if (items.size() <= this.countersSize) {
      counter.setError(frequency);
    }
  }
}
