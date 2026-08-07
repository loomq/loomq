package com.loomq.infrastructure.wheel;
public record TailEntry(long executeAtMs, byte[] encodedSlot) {}
