package data;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record OpenMeteoResponse(
    @JsonProperty("current") CurrentData current
) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record CurrentData(
        @JsonProperty("temperature_2m") Double temperature,
        @JsonProperty("relative_humidity_2m") Integer humidity,
        @JsonProperty("wind_speed_10m") Double windSpeed,
        @JsonProperty("time") String time
    ) {}
}