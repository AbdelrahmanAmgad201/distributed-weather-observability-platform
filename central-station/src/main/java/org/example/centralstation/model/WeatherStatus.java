package org.example.centralstation.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WeatherStatus {
    @JsonProperty("station_id")
    private Long stationId;

    @JsonProperty("s_no")
    private Long sNo;

    @JsonProperty("battery_status")
    private BatteryStatus batteryStatus;

    @JsonProperty("status_timestamp")
    private Long statusTimestamp;

    private Weather weather;
}