package dev.intermediatebox.analyticsapi.aggregator.topk.spaceSaving;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.LinkedList;

@Getter
@Setter
@ToString
public class Bucket<T> {
  private long frequency;

  private LinkedList<Counter<T>> children = new LinkedList<>();

  public Bucket(long frequency) {
    this.frequency = frequency;
  }
}
