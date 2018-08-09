package com.ps.mapreducedemo.domain;

import lombok.Value;

@Value
public class WordFrequency {
    private final long lineId;
    private final String word;
    private final long count;
}
