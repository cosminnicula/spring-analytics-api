package dev.intermediatebox.analyticsapi.query;

import dev.intermediatebox.analyticsapi.aggregator.topk.spaceSaving.Counter;
import dev.intermediatebox.analyticsapi.query.api.QueryTopKApiEntity;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@Component
public class QueryMapper {
  @Value("${dateTime.pattern}")
  private String dateTimePattern;

  public String toRawRecord(QueryEntity queryEntity) {
    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(dateTimePattern);

    return String.join("\t",
        queryEntity.getDateTime().format(dateTimeFormatter),
        queryEntity.getQuery(),
        String.valueOf(queryEntity.getTimestamp()),
        String.valueOf(queryEntity.getQueryHash())
    );
  }

  public QueryEntity toQueryEntity(String rawRecord) {
    QueryEntity.QueryEntityBuilder queryEntity = QueryEntity.builder();
    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(dateTimePattern);
    String[] data = rawRecord.split("\t");

    queryEntity.dateTime(LocalDateTime.parse(data[0], dateTimeFormatter));
    queryEntity.query(data[1]);
    queryEntity.timestamp(Long.valueOf(data[2]));
    queryEntity.queryHash(Long.valueOf(data[3]));

    return queryEntity.build();
  }

  public Map<String, LocalDateTime> toStartEndDates(String rawStartDate) {
    // sanity check, no date-aware validation
    Pattern testPattern = Pattern.compile("\\d{4}(-\\d{2}(-\\d{2}( \\d{2}(:\\d{2})?)?)?)?");
    if (!testPattern.matcher(rawStartDate).matches()) {
      return null;
    }

    // infer missing pieces and construct start and end dates
    String inferredStartDate = "";
    String inferredEndDate = "";
    switch (rawStartDate.length()) {
      case 4: // yyyy
        inferredStartDate = String.format("%s-01-01 00:00:00", rawStartDate);
        inferredEndDate = String.format("%s-12-31 23:59:59", rawStartDate);
        break;
      case 7: // yyyy-DD
        inferredStartDate = String.format("%s-01 00:00:00", rawStartDate);
        inferredEndDate = String.format("%s-31 23:59:59", rawStartDate);
        break;
      case 10: // yyyy-DD-mm
        inferredStartDate = String.format("%s 00:00:00", rawStartDate);
        inferredEndDate = String.format("%s 23:59:59", rawStartDate);
        break;
      case 13: // yyyy-DD-mm HH
        inferredStartDate = String.format("%s:00:00", rawStartDate);
        inferredEndDate = String.format("%s:59:59", rawStartDate);
        break;
      case 16: // yyyy-DD-mm HH:mm
        inferredStartDate = String.format("%s:00", rawStartDate);
        inferredEndDate = String.format("%s:59", rawStartDate);
        break;
    }

    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(dateTimePattern);
    LocalDateTime startDate = LocalDateTime.parse(inferredStartDate, dateTimeFormatter);
    LocalDateTime endDate = LocalDateTime.parse(inferredEndDate, dateTimeFormatter);

    return Map.of("startDate", startDate, "endDate", endDate);
  }

  public List<Map<String, Integer>> toQueryTopKServiceEntity(List<Counter<String>> counters) {
    List<Map<String, Integer>> topKQueries = new ArrayList<>();

    counters.stream().forEach(counter -> {
      topKQueries.add(Map.of(counter.getItem(), Math.toIntExact(counter.getFrequency())));
    });

    return topKQueries;
  }

  public Map<String, List<QueryTopKApiEntity>> toQueryTopKApiEntity(List<Map<String, Integer>> topKQueries) {
    List<QueryTopKApiEntity> queryTopKApiEntities = new ArrayList<>();

    for (Map<String, Integer> groupedQueries : topKQueries) {
      for (Map.Entry<String, Integer> query : groupedQueries.entrySet()) {
        QueryTopKApiEntity queryTopKApiEntity = QueryTopKApiEntity.builder()
            .query(query.getKey())
            .count(query.getValue())
            .build();
        queryTopKApiEntities.add(queryTopKApiEntity);
      }
    }

    return Map.of("queries", queryTopKApiEntities);
  }

  public Map<String, Long> toQueryDistinctCountApiEntity(long count) {
    return Map.of("count", count);
  }
}
