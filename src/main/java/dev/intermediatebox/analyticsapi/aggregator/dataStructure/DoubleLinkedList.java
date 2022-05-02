package dev.intermediatebox.analyticsapi.aggregator.dataStructure;

import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@NoArgsConstructor
@Getter
public class DoubleLinkedList<T> implements Iterable<T> {

  public static class Node<T> {
    private T item;

    private Node next;

    private Node prev;

    public Node(T item) {
      this.item = item;
    }

    public T getItem() {
      return item;
    }

    public Node<T> getNext() {
      return next;
    }

    public Node<T> getPrev() {
      return prev;
    }
  }

  private Node<T> head;

  private Node<T> tail;

  public synchronized void insertBefore(Node parent, Node node) {
    node.prev = parent.prev;
    node.next = parent;
    if (parent.prev == null) {
      head = node;
    } else {
      parent.prev.next = node;
    }
    parent.prev = node;
  }

  public synchronized void insertAfter(Node parent, Node node) {
    node.prev = parent;
    node.next = parent.next;
    if (parent.next == null) {
      tail = node;
    } else {
      parent.next.prev = node;
    }
    parent.next = node;
  }

  public synchronized void insertFirst(Node node) {
    if (head == null) {
      head = node;
      tail = node;
      node.prev = null;
      node.next = null;
    } else {
      insertBefore(head, node);
    }
  }

  public synchronized void insertLast(Node node) {
    if (tail == null) {
      insertFirst(node);
    } else {
      insertAfter(tail, node);
    }
  }

  public synchronized void remove(Node node) {
    if (node.prev == null) {
      head = node.next;
    } else {
      node.prev.next = node.next;
    }
    if (node.next == null) {
      tail = node.prev;
    } else {
      node.next.prev = node.prev;
    }
  }

  public Stream<T> stream() {
    return StreamSupport.stream(spliterator(), false);
  }

  @Override
  public Iterator<T> iterator() {
    return new Iterator<T>() {
      private DoubleLinkedList.Node<T> curr = head;

      public boolean hasNext() {
        return curr != null;
      }

      public T next() {
        Node<T> temp = curr;
        curr = curr.next;
        return temp.getItem();
      }
    };
  }
}
