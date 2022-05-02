package dev.intermediatebox.analyticsapi.application;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springdoc.core.GroupedOpenApi;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SwaggerConfig {
  @Bean
  public GroupedOpenApi apiGroup() {
    return GroupedOpenApi
        .builder()
        .group("Api")
        .pathsToMatch("/api/**")
        .build();
  }

  @Bean
  public OpenAPI apiInfo() {
    return new OpenAPI().info(
        new Info()
            .title("IntermediateBox Analytics Api")
            .description("IntermediateBox Analytics Api")
            .version("1.0")
    );
  }
}
