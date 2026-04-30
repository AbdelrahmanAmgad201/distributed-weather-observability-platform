package data;

import com.fasterxml.jackson.annotation.JsonProperty;

public record WeatherStatus(
        @JsonProperty("station_id") Long stationId,
        @JsonProperty("s_no") Long sNo,
        @JsonProperty("battery_status") BatteryStatus batteryStatus,
        @JsonProperty("status_timestamp") Long statusTimestamp,
        Weather weather
) {}