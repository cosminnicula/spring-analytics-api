package dev.intermediatebox.analyticsapi.query.api;

import lombok.*;

@NoArgsConstructor
@AllArgsConstructor
@Builder
@Getter
@ToString
public class QueryTopKApiEntity {
  private String query;

  private long count;
}
