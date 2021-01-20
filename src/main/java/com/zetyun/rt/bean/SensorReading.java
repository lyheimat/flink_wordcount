package com.zetyun.rt.bean;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SensorReading {
    private String id;
    private Long ts;
    private Double temp;
}
