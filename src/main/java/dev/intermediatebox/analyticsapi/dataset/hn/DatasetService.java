package dev.intermediatebox.analyticsapi.dataset.hn;

import dev.intermediatebox.analyticsapi.query.QueryEntity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class DatasetService {
  @Autowired
  private DatasetRepository datasetRepository;

  // in-memory cache
  // TODO: cache invalidation
  private List<QueryEntity> data = new ArrayList<>();

  public List<QueryEntity> getData(LocalDateTime startDate, LocalDateTime endDate) {
    if (data.isEmpty()) {
      data = datasetRepository.getQueryEntities(startDate, endDate);
    }

    return data;
  }
}
