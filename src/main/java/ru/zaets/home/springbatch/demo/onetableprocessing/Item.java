package ru.zaets.home.springbatch.demo.onetableprocessing;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Item {
    private Long id;
    private boolean processed = false;
}
