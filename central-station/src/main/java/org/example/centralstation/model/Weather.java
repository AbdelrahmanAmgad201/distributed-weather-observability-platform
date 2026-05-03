package org.example.centralstation.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Weather {
    private Integer humidity;
    private Integer temperature;

    @JsonProperty("wind_speed")
    private Integer windSpeed;
}