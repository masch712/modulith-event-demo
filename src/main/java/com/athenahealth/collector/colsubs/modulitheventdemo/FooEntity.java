package com.athenahealth.collector.colsubs.modulitheventdemo;

import jakarta.annotation.Nullable;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Entity
@Data
@AllArgsConstructor
@NoArgsConstructor
public class FooEntity {
  @Id
  UUID id;

  @Nullable
  Instant handledAt;

}
