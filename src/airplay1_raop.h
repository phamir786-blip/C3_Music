#pragma once
#include <Arduino.h>

bool airplay1Start(uint16_t port);
void airplay1Loop();
void airplay1Stop();
bool airplay1IsRunning();
