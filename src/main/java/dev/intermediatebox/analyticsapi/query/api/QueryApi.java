package dev.intermediatebox.analyticsapi.query.api;

import dev.intermediatebox.analyticsapi.query.QueryMapper;
import dev.intermediatebox.analyticsapi.query.QueryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping(path = "api/query")
@RequiredArgsConstructor
@Slf4j
public class QueryApi {

  private final QueryService queryService;

  private final QueryMapper queryMapper;

  @Value("${dateTime.pattern}")
  private String dateTimePattern;

  @GetMapping("count/{startDate}")
  public ResponseEntity<Map> getDistinctCount(@PathVariable String startDate) {
    var startEndDates = queryMapper.toStartEndDates(startDate);
    if (startEndDates != null) {
      var count =
          queryService.getDistinctCount(startEndDates.get("startDate"), startEndDates.get("endDate"));
      return ResponseEntity.ok().body(queryMapper.toQueryDistinctCountApiEntity(count));
    }

    return ResponseEntity.badRequest().build();
  }

  @GetMapping("popular/{startDate}")
  public ResponseEntity<Map> getTopK(@PathVariable String startDate, @RequestParam Integer k) {
    var startEndDates = queryMapper.toStartEndDates(startDate);
    if (startEndDates != null) {
      var topKQueries =
          queryService.getTopKQueries(startEndDates.get("startDate"), startEndDates.get("endDate"), k);
      return ResponseEntity.ok().body(queryMapper.toQueryTopKApiEntity(topKQueries));
    }

    return ResponseEntity.badRequest().build();
  }
}
