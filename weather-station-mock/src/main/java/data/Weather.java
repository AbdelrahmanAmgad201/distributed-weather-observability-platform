package data;

import com.fasterxml.jackson.annotation.JsonProperty;

public record Weather(
        Integer humidity,
        Integer temperature,
        @JsonProperty("wind speed") Integer windSpeed
) {}