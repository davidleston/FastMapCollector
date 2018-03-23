package com.davidleston.stream;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collector;

import static java.util.function.Function.identity;

/**
 * A fast map collector for when you're interested in keeping the first or last elements with duplicate keys.
 */
public enum FastMapCollector {

  /*
   * If the mapped keys contains duplicates (according to {@link Object#equals(Object)}), the first element
   * in encounter order is retained. The {@code Map} is created by a provided supplier function.
   */
  keepFirst() {
    @Override
    protected <K, T, U> BiConsumer<Map<K, U>, T> accumulator(Function<? super T, ? extends K> keyMapper,
                                                             Function<? super T, ? extends U> valueMapper) {
      return (kuMap, t) -> kuMap.putIfAbsent(keyMapper.apply(t), valueMapper.apply(t));
    }

    @Override
    protected <K, U> BinaryOperator<Map<K, U>> combiner() {
      return (kuMap, kuMap2) -> {
        kuMap2.putAll(kuMap);
        return kuMap2;
      };
    }
  },

  /*
   * If the mapped keys contains duplicates (according to {@link Object#equals(Object)}), the last element
   * in encounter order is retained. The {@code Map} is created by a provided supplier function.
   */
  keepLast() {
    @Override
    protected <K, T, U> BiConsumer<Map<K, U>, T> accumulator(Function<? super T, ? extends K> keyMapper, Function<? super T, ? extends U> valueMapper) {
      return (kuMap, t) -> kuMap.put(keyMapper.apply(t), valueMapper.apply(t));
    }

    protected @Override
    <K, U> BinaryOperator<Map<K, U>> combiner() {
      return (kuMap, kuMap2) -> {
        kuMap.putAll(kuMap2);
        return kuMap;
      };
    }
  };

  /**
   * Returns a {@code Collector} that accumulates elements into a
   * {@code Map} whose keys and values are the result of applying the provided
   * mapping functions to the input elements.
   *
   * @implNote
   * The returned {@code Collector} is not concurrent. For parallel stream
   * pipelines, the {@code combiner} function operates by merging the keys
   * from one map into another, which can be an expensive operation.
   *
   * @param <T> the type of the input elements
   * @param <K> the output type of the key mapping function
   * @param keyMapper a mapping function to produce keys
   * @return a {@code Collector} which collects elements into a {@code Map}
   */
  public <T, K> Collector<T, ?, Map<K, T>> toMap(Function<? super T, ? extends K> keyMapper) {
    return toMap(keyMapper, identity());
  }

  /**
   * Returns a {@code Collector} that accumulates elements into a
   * {@code Map} whose keys and values are the result of applying the provided
   * mapping functions to the input elements.
   *
   * @implNote
   * The returned {@code Collector} is not concurrent. For parallel stream
   * pipelines, the {@code combiner} function operates by merging the keys
   * from one map into another, which can be an expensive operation.
   *
   * @param <T> the type of the input elements
   * @param <K> the output type of the key mapping function
   * @param <U> the output type of the value mapping function
   * @param keyMapper a mapping function to produce keys
   * @param valueMapper a mapping function to produce values
   * @return a {@code Collector} which collects elements into a {@code Map}
   */
  public <T, K, U> Collector<T, ?, Map<K, U>> toMap(Function<? super T, ? extends K> keyMapper,
                                                    Function<? super T, ? extends U> valueMapper) {
    return Collector.of(
        HashMap::new,
        accumulator(keyMapper, valueMapper),
        combiner(),
        Collector.Characteristics.IDENTITY_FINISH);
  }

  abstract protected <K, T, U> BiConsumer<Map<K, U>, T> accumulator(Function<? super T, ? extends K> keyMapper,
                                                                    Function<? super T, ? extends U> valueMapper);
  abstract protected <K, U> BinaryOperator<Map<K, U>> combiner();
}
