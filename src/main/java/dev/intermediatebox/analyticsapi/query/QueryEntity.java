package dev.intermediatebox.analyticsapi.query;

import lombok.*;

import java.time.LocalDateTime;

@NoArgsConstructor
@AllArgsConstructor
@Builder
@Getter
@ToString
public class QueryEntity {
  private LocalDateTime dateTime;

  private String query;

  private long timestamp;

  private long queryHash;
}
