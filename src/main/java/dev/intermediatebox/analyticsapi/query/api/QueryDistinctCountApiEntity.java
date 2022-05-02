package dev.intermediatebox.analyticsapi.query.api;

import lombok.*;

@NoArgsConstructor
@AllArgsConstructor
@Builder
@Getter
@ToString
public class QueryDistinctCountApiEntity {
  private long count;
}
