package dev.intermediatebox.analyticsapi.aggregator.topk.spaceSaving;

import dev.intermediatebox.analyticsapi.aggregator.dataStructure.DoubleLinkedList;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class Counter<T> {
  private long frequency;

  private long error;

  private T item;

  private DoubleLinkedList.Node<Bucket<T>> parent;

  public Counter(DoubleLinkedList.Node<Bucket<T>> parent) {
    this.parent = parent;
    this.frequency = 0;
    this.error = 0;
    this.item = null;
  }
}
