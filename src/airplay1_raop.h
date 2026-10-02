#pragma once
#include <Arduino.h>

typedef bool (*Airplay1PcmSink)(const uint8_t* data, size_t len);

void airplay1SetPcmSink(Airplay1PcmSink sink);
bool airplay1Start(uint16_t port);
void airplay1Loop();
void airplay1Stop();
bool airplay1IsRunning();
