package org.example.centralstation.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record Weather(
        Integer humidity,
        Integer temperature,
        @JsonProperty("wind_speed") Integer windSpeed
) {}