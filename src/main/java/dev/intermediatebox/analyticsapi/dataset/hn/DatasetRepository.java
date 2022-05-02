package dev.intermediatebox.analyticsapi.dataset.hn;

import dev.intermediatebox.analyticsapi.query.QueryEntity;
import dev.intermediatebox.analyticsapi.query.QueryMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Repository
@Slf4j
public class DatasetRepository {
  @Value("${dataset.baseDir}")
  private String datasetBaseDir;

  @Value("${dateTime.pattern}")
  private String dateTimePattern;

  @Autowired
  private QueryMapper queryMapper;

  private List<QueryEntity> queryEntities = new ArrayList<>();

  public List<QueryEntity> getQueryEntities(LocalDateTime startDate, LocalDateTime endDate) {
    DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(this.dateTimePattern);
    long startDateEpochSeconds = startDate.atZone(ZoneId.systemDefault()).toInstant().getEpochSecond();
    long endDateEpochSeconds = endDate.atZone(ZoneId.systemDefault()).toInstant().getEpochSecond();

    try (Stream<Path> paths = Files.walk(Paths.get(datasetBaseDir))) {
      paths
          .filter(Files::isRegularFile)
          .filter(path -> {
            LocalDateTime dateTime = LocalDateTime.parse(
                String.format("%s 00:00:00", path.getFileName().toString()), dateFormatter
            );
            return isBetweenStartEndDate(dateTime, startDate, endDate);
          })
          .forEach(path -> {
            try {
              this.queryEntities.addAll(Files.readAllLines(Path.of(String.format("%s/%s", datasetBaseDir, path.getFileName().toString()))).stream()
                  .map(rawLine -> queryMapper.toQueryEntity(rawLine))
                  .filter(queryEntity -> isBetweenStartEndDateTime(queryEntity.getTimestamp(), startDateEpochSeconds, endDateEpochSeconds))
                  .collect(Collectors.toList()));
              log.info(String.format("Read next %s queries.", this.queryEntities.size()));
            } catch (IOException e) {
              throw new RuntimeException("Cannot read chunk.");
            }
          });
    } catch (IOException e) {
      throw new RuntimeException("Cannot iterate chunks.");
    }

    return queryEntities;
  }

  private boolean isBetweenStartEndDate(LocalDateTime dateTime, LocalDateTime startDate, LocalDateTime endDate) {
    return dateTime.getDayOfYear() >= startDate.getDayOfYear() &&
        dateTime.getDayOfYear() <= endDate.getDayOfYear();
  }

  private boolean isBetweenStartEndDateTime(long epochSeconds, long startDateEpochSeconds, long endDateEpochSeconds) {
    return epochSeconds >= startDateEpochSeconds &&
        epochSeconds <= endDateEpochSeconds;
  }
}
