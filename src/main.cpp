/*
  C3 High-Performance Music Receiver — Firmware v1.4.0
  Hardware: ESP32-C3 + SSD1306 OLED (72x40) + UDA1334A I2S DAC
  Features:
    - 16-bit & 24-bit Audio @ 44.1kHz & 48kHz
    - 128 KB Massive Ring Buffer (Zero-Choppy Engine)
    - TCP Push Server (50005) + Outbound TCP + HTTP Stream Client Fallback
    - Full Web UI: Status, Controls, Settings & Browser OTA Upload
  Pinout:
    - OLED: SDA = GPIO 5, SCL = GPIO 6
    - I2S:  BCLK = GPIO 3, WS/LRCLK = GPIO 1, DOUT = GPIO 10
*/

#include <Arduino.h>
#include <WiFi.h>
#include <WiFiClient.h>
#include <WebServer.h>
#include <ESPmDNS.h>
#include <Preferences.h>
#include <Update.h>
#include <U8g2lib.h>
#include <Wire.h>
#include <driver/i2s.h>

// --- Configuration Defaults ---
static const char WIFI_SSID[] = "GFiber_2.4_Coverage_AECD9";
static const char WIFI_PASSWORD[] = "006BF4FD";
static const char PHONE_HOST[] = "192.168.254.119";

static const char DEVICE_NAME[] = "C3 Music Receiver";
static const char MDNS_HOSTNAME[] = "c3music";
static const char FIRMWARE_VERSION[] = "1.4.0";

static constexpr uint8_t OLED_SDA = 5;
static constexpr uint8_t OLED_SCL = 6;
static constexpr uint8_t OLED_WIDTH = 72;
static constexpr uint8_t OLED_HEIGHT = 40;

static constexpr int I2S_BCLK_PIN = 3;
static constexpr int I2S_LRCLK_PIN = 1;
static constexpr int I2S_DOUT_PIN = 10;

static constexpr uint16_t TCP_DEFAULT_PORT = 50005;
static constexpr uint16_t HTTP_DEFAULT_PORT = 8080;

static constexpr uint32_t DEFAULT_SAMPLE_RATE = 44100;
static constexpr uint8_t DEFAULT_CHANNELS = 2;
static constexpr uint8_t DEFAULT_BITS_PER_SAMPLE = 16;

// 128 KB Audio Ring Buffer (~450ms cushion)
static constexpr size_t AUDIO_RING_BYTES = 131072;
static constexpr size_t NETWORK_READ_BYTES = 1460;
static constexpr size_t I2S_WRITE_BYTES = 2048;
static constexpr uint32_t PREBUFFER_BYTES = 16000;

static constexpr uint32_t STREAM_RETRY_MS = 2500;
static constexpr uint32_t WIFI_RETRY_MS = 10000;
static constexpr uint32_t OLED_REFRESH_MS = 1000;
static constexpr uint32_t HTTP_HEADER_TIMEOUT_MS = 6000;
static constexpr uint32_t STREAM_READ_TIMEOUT_MS = 15000;

enum StreamMode : uint8_t { STREAM_MODE_TCP = 0, STREAM_MODE_HTTP = 1 };
enum ReceiverState : uint8_t {
  RX_BOOTING = 0, RX_WIFI_CONNECTING, RX_WIFI_OFFLINE, RX_IDLE,
  RX_CONNECTING, RX_BUFFERING, RX_STREAMING, RX_STOPPED, RX_ERROR, RX_UPDATING
};

struct StreamFormat {
  uint32_t sampleRate = DEFAULT_SAMPLE_RATE;
  uint16_t channels = DEFAULT_CHANNELS;
  uint16_t bitsPerSample = DEFAULT_BITS_PER_SAMPLE;
  uint16_t audioFormat = 1;
  bool valid = false;
};

struct RuntimeStats {
  uint32_t reconnects = 0, underruns = 0, streamErrors = 0;
  uint32_t bytesReceived = 0, bytesPlayed = 0, sessionStartedMs = 0, lastReceiveMs = 0;
  String lastError;
};

struct Settings {
  String phoneHost;
  uint16_t tcpPort = TCP_DEFAULT_PORT;
  uint16_t httpPort = HTTP_DEFAULT_PORT;
  StreamMode preferredMode = STREAM_MODE_TCP;
  bool autoFallback = true, autoReconnect = true, oledEnabled = true, streamEnabled = true;
  uint8_t volumePercent = 50;
};

// Global System Objects
static U8G2_SSD1306_72X40_ER_F_HW_I2C oled(U8G2_R0, U8X8_PIN_NONE);
static bool oledAvailable = false;
static Preferences preferences;
static WebServer server(80);
static WiFiServer tcpPushServer(TCP_DEFAULT_PORT);
static WiFiClient streamClient;
static Settings settings;
static StreamFormat streamFormat;
static RuntimeStats stats;

static volatile ReceiverState receiverState = RX_BOOTING;
static volatile bool stopRequested = false;
static volatile bool i2sReady = false;
static volatile bool bufferStarted = false;

static uint8_t audioRing[AUDIO_RING_BYTES];
static volatile size_t ringReadIndex = 0, ringWriteIndex = 0, ringCount = 0;
static portMUX_TYPE ringMux = portMUX_INITIALIZER_UNLOCKED;

static SemaphoreHandle_t audioDataSemaphore = nullptr;
static SemaphoreHandle_t i2sMux = nullptr;
static SemaphoreHandle_t streamClientMux = nullptr;
static TaskHandle_t streamTaskHandle = nullptr, playbackTaskHandle = nullptr;

static uint32_t lastWifiAttemptMs = 0, lastStreamAttemptMs = 0, lastOledRefreshMs = 0;
static bool mdnsStarted = false, tcpPushServerStarted = false;
static uint32_t activeI2sSampleRate = 0;
static uint16_t activeI2sBits = 0;

static void loadSettings() {
  preferences.begin("c3music", false);
  settings.phoneHost = preferences.getString("host", PHONE_HOST);
  settings.tcpPort = preferences.getUShort("tcpport", TCP_DEFAULT_PORT);
  settings.httpPort = preferences.getUShort("httpport", HTTP_DEFAULT_PORT);
  settings.preferredMode = (StreamMode)preferences.getUChar("mode", STREAM_MODE_TCP);
  settings.autoFallback = preferences.getBool("fallback", true);
  settings.autoReconnect = preferences.getBool("autorecon", true);
  settings.oledEnabled = preferences.getBool("oled", true);
  settings.streamEnabled = preferences.getBool("enabled", true);
  settings.volumePercent = preferences.getUChar("volume", 50);
  if (settings.volumePercent > 100) settings.volumePercent = 100;
  if (settings.phoneHost.length() == 0 || settings.phoneHost == "192.168.1.100") {
    settings.phoneHost = PHONE_HOST;
    preferences.putString("host", settings.phoneHost);
  }
}

static void saveSettings() {
  preferences.putString("host", settings.phoneHost);
  preferences.putUShort("tcpport", settings.tcpPort);
  preferences.putUShort("httpport", settings.httpPort);
  preferences.putUChar("mode", settings.preferredMode);
  preferences.putBool("fallback", settings.autoFallback);
  preferences.putBool("autorecon", settings.autoReconnect);
  preferences.putBool("oled", settings.oledEnabled);
  preferences.putBool("enabled", settings.streamEnabled);
  preferences.putUChar("volume", settings.volumePercent);
}

static const char* receiverStateName(ReceiverState v) {
  switch(v) {
    case RX_BOOTING: return "Booting";
    case RX_WIFI_CONNECTING: return "WiFi connecting";
    case RX_WIFI_OFFLINE: return "WiFi offline";
    case RX_IDLE: return "Ready";
    case RX_CONNECTING: return "Connecting";
    case RX_BUFFERING: return "Buffering";
    case RX_STREAMING: return "Streaming";
    case RX_STOPPED: return "Stopped";
    case RX_ERROR: return "Error";
    case RX_UPDATING: return "Updating";
    default: return "Unknown";
  }
}

static void setReceiverState(ReceiverState value, const String &error = "") {
  receiverState = value;
  if (error.length() > 0) {
    stats.lastError = error;
    Serial.printf("[STATE] %s: %s\n", receiverStateName(value), error.c_str());
  } else {
    Serial.printf("[STATE] %s\n", receiverStateName(value));
  }
}

// Thread-safe circular ring buffer (Guaranteed NO volatile std::min conflict)
static inline bool ringWrite(const uint8_t *data, size_t length) {
  if (!data || length == 0) return true;
  bool ok = true;
  portENTER_CRITICAL(&ringMux);
  size_t currentCount = (size_t)ringCount;
  size_t freeBytes = AUDIO_RING_BYTES - currentCount;
  if (length > freeBytes) { length = freeBytes; ok = false; }

  size_t currentWrite = (size_t)ringWriteIndex;
  size_t spaceToEnd = AUDIO_RING_BYTES - currentWrite;
  size_t first = (length < spaceToEnd) ? length : spaceToEnd;
  memcpy(audioRing + currentWrite, data, first);

  size_t second = length - first;
  if (second > 0) memcpy(audioRing, data + first, second);

  ringWriteIndex = (currentWrite + length) % AUDIO_RING_BYTES;
  ringCount = currentCount + length;
  portEXIT_CRITICAL(&ringMux);
  if (length > 0 && audioDataSemaphore) xSemaphoreGive(audioDataSemaphore);
  return ok;
}

static inline size_t ringRead(uint8_t *out, size_t maxLen) {
  if (!out || maxLen == 0) return 0;
  portENTER_CRITICAL(&ringMux);
  size_t currentCount = (size_t)ringCount;
  size_t len = (maxLen < currentCount) ? maxLen : currentCount;
  if (len > 0) {
    size_t currentRead = (size_t)ringReadIndex;
    size_t spaceToEnd = AUDIO_RING_BYTES - currentRead;
    size_t first = (len < spaceToEnd) ? len : spaceToEnd;
    memcpy(out, audioRing + currentRead, first);

    size_t second = len - first;
    if (second > 0) memcpy(out + first, audioRing, second);

    ringReadIndex = (currentRead + len) % AUDIO_RING_BYTES;
    ringCount = currentCount - len;
  }
  portEXIT_CRITICAL(&ringMux);
  return len;
}

static inline void ringClear() {
  portENTER_CRITICAL(&ringMux);
  ringReadIndex = ringWriteIndex = ringCount = 0;
  portEXIT_CRITICAL(&ringMux);
  bufferStarted = false;
}

static inline size_t ringSize() {
  portENTER_CRITICAL(&ringMux);
  size_t v = (size_t)ringCount;
  portEXIT_CRITICAL(&ringMux);
  return v;
}

static inline uint8_t ringPercent() {
  return (uint8_t)((ringSize() * 100UL) / AUDIO_RING_BYTES);
}

// Display routines
static void displayBegin() {
  Wire.begin(OLED_SDA, OLED_SCL, 400000);
  oled.begin();
  oled.setPowerSave(settings.oledEnabled ? 0 : 1);
  oled.clearBuffer();
  oled.setFont(u8g2_font_5x7_tf);
  oled.drawStr(0, 8, "C3 MUSIC v1.4");
  oled.drawStr(0, 22, "Starting...");
  oled.sendBuffer();
  oledAvailable = true;
}

static void displayUpdate() {
  if (!oledAvailable || !settings.oledEnabled) return;
  // Quiet I2C during streaming to prevent bus contention
  if (receiverState == RX_STREAMING) return;
  if (millis() - lastOledRefreshMs < OLED_REFRESH_MS) return;
  lastOledRefreshMs = millis();

  oled.clearBuffer();
  oled.setFont(u8g2_font_5x7_tf);
  oled.drawStr(0, 8, "C3 MUSIC");
  const char *st = (receiverState == RX_BUFFERING) ? "BUFFER" : receiverStateName(receiverState);
  oled.drawStr(OLED_WIDTH - oled.getStrWidth(st), 8, st);

  oled.setFont(u8g2_font_6x10_tf);
  if (WiFi.status() == WL_CONNECTED) {
    oled.drawStr(0, 25, WiFi.localIP().toString().c_str());
  } else {
    oled.drawStr(0, 25, "No WiFi");
  }
  oled.sendBuffer();
}

static bool beginI2S(uint32_t sr, uint16_t ch, uint16_t bits) {
  if ((bits != 16 && bits != 24) || (ch != 1 && ch != 2)) return false;
  if (i2sMux) xSemaphoreTake(i2sMux, portMAX_DELAY);

  if (i2sReady && activeI2sSampleRate == sr && activeI2sBits == bits) {
    if (i2sMux) xSemaphoreGive(i2sMux);
    return true;
  }

  if (i2sReady) {
    i2s_driver_uninstall(I2S_NUM_0);
    i2sReady = false;
  }

  i2s_bits_per_sample_t bps = (bits == 24) ? I2S_BITS_PER_SAMPLE_32BIT : I2S_BITS_PER_SAMPLE_16BIT;

  i2s_config_t cfg = {
    .mode = (i2s_mode_t)(I2S_MODE_MASTER | I2S_MODE_TX),
    .sample_rate = sr,
    .bits_per_sample = bps,
    .channel_format = I2S_CHANNEL_FMT_RIGHT_LEFT,
    .communication_format = I2S_COMM_FORMAT_STAND_I2S,
    .intr_alloc_flags = ESP_INTR_FLAG_LEVEL1,
    .dma_buf_count = 16,        // 16 buffers = deep jitter cushion
    .dma_buf_len = 512,
    .use_apll = false,
    .tx_desc_auto_clear = true,
    .fixed_mclk = 0
  };

  if (i2s_driver_install(I2S_NUM_0, &cfg, 0, nullptr) != ESP_OK) {
    if (i2sMux) xSemaphoreGive(i2sMux);
    return false;
  }

  i2s_pin_config_t pin = {
    .bck_io_num = I2S_BCLK_PIN,
    .ws_io_num = I2S_LRCLK_PIN,
    .data_out_num = I2S_DOUT_PIN,
    .data_in_num = I2S_PIN_NO_CHANGE
  };

  if (i2s_set_pin(I2S_NUM_0, &pin) != ESP_OK) {
    i2s_driver_uninstall(I2S_NUM_0);
    if (i2sMux) xSemaphoreGive(i2sMux);
    return false;
  }

  i2s_zero_dma_buffer(I2S_NUM_0);
  activeI2sSampleRate = sr;
  activeI2sBits = bits;
  i2sReady = true;
  if (i2sMux) xSemaphoreGive(i2sMux);

  Serial.printf("[I2S] Running %lu Hz, %u-bit, %s\n", (unsigned long)sr, bits, ch == 2 ? "Stereo" : "Mono");
  return true;
}

// DRAM Buffers: 0 stack usage
static uint8_t playbackIn[I2S_WRITE_BYTES / 2];
static uint8_t playbackOut[I2S_WRITE_BYTES * 2];

static void playbackTask(void*) {
  for (;;) {
    if (!i2sReady || !bufferStarted) {
      vTaskDelay(pdMS_TO_TICKS(8));
      continue;
    }

    size_t n = ringRead(playbackIn, sizeof(playbackIn));
    if (n == 0) {
      stats.underruns++;
      if (ringSize() < 2048) {
        bufferStarted = false;
        setReceiverState(RX_BUFFERING);
      }
      if (audioDataSemaphore) xSemaphoreTake(audioDataSemaphore, pdMS_TO_TICKS(15));
      else vTaskDelay(pdMS_TO_TICKS(3));
      continue;
    }

    const uint8_t *writeBuf = playbackIn;
    size_t writeLen = n;
    const uint8_t volume = settings.volumePercent;

    if (streamFormat.bitsPerSample == 16) {
      if (volume < 100) {
        int16_t *samples = reinterpret_cast<int16_t*>(playbackIn);
        size_t count = n / sizeof(int16_t);
        for (size_t i = 0; i < count; ++i) {
          samples[i] = (int16_t)(((int32_t)samples[i] * volume) / 100);
        }
      }

      if (streamFormat.channels == 1) {
        size_t count = n / sizeof(int16_t);
        int16_t *src = reinterpret_cast<int16_t*>(playbackIn);
        int16_t *dst = reinterpret_cast<int16_t*>(playbackOut);
        for (size_t i = 0; i < count; ++i) {
          dst[i * 2] = src[i];
          dst[i * 2 + 1] = src[i];
        }
        writeBuf = playbackOut;
        writeLen = count * 4;
      }
    } 
    else if (streamFormat.bitsPerSample == 24) {
      size_t count = n / 3;
      int32_t *dst32 = reinterpret_cast<int32_t*>(playbackOut);

      for (size_t i = 0; i < count; ++i) {
        size_t idx = i * 3;
        int32_t s = (int32_t)((uint32_t)playbackIn[idx] |
                             ((uint32_t)playbackIn[idx + 1] << 8) |
                             ((uint32_t)playbackIn[idx + 2] << 16));
        if (s & 0x00800000) s |= 0xFF000000;
        if (volume < 100) s = (s * volume) / 100;
        dst32[i] = s << 8;
      }
      writeBuf = playbackOut;
      writeLen = count * 4;
    }

    if (i2sMux) xSemaphoreTake(i2sMux, portMAX_DELAY);
    size_t w = 0;
    esp_err_t r = i2sReady ? i2s_write(I2S_NUM_0, writeBuf, writeLen, &w, pdMS_TO_TICKS(40)) : ESP_FAIL;
    if (i2sMux) xSemaphoreGive(i2sMux);

    if (r == ESP_OK && w > 0) stats.bytesPlayed += n;
    else vTaskDelay(pdMS_TO_TICKS(2));
  }
}

static void runConnectedStream() {
  uint8_t in[NETWORK_READ_BYTES];
  stats.sessionStartedMs = millis();
  stats.lastReceiveMs = millis();
  ringClear();
  setReceiverState(RX_BUFFERING);

  uint32_t waitStart = millis();
  while (streamClient.connected() && streamClient.available() < 16 && (millis() - waitStart < 300)) {
    delay(2);
  }

  uint32_t sr = DEFAULT_SAMPLE_RATE;
  uint16_t bits = DEFAULT_BITS_PER_SAMPLE;
  uint16_t ch = DEFAULT_CHANNELS;

  if (streamClient.available() >= 16) {
    uint8_t hdr[16];
    if (streamClient.readBytes(hdr, 16) == 16) {
      if (hdr[0] == 'C' && hdr[1] == '3' && hdr[2] == 'M' && hdr[3] == 'S') {
        bits = hdr[5];
        ch = hdr[6];
        sr = (uint32_t)hdr[8] | ((uint32_t)hdr[9] << 8) | ((uint32_t)hdr[10] << 16) | ((uint32_t)hdr[11] << 24);
      } else {
        ringWrite(hdr, 16);
        stats.bytesReceived += 16;
      }
    }
  }

  if (sr < 8000 || sr > 96000) sr = DEFAULT_SAMPLE_RATE;
  if (bits != 16 && bits != 24) bits = DEFAULT_BITS_PER_SAMPLE;
  if (ch != 1 && ch != 2) ch = DEFAULT_CHANNELS;

  streamFormat.sampleRate = sr;
  streamFormat.bitsPerSample = bits;
  streamFormat.channels = ch;
  streamFormat.valid = true;

  beginI2S(sr, ch, bits);

  while (!stopRequested && streamClient.connected() && WiFi.status() == WL_CONNECTED) {
    int av = streamClient.available();
    if (av > 0) {
      size_t availBytes = (size_t)av;
      size_t w = (availBytes < sizeof(in)) ? availBytes : sizeof(in);
      int n = streamClient.read(in, w);
      if (n > 0) {
        stats.bytesReceived += n;
        stats.lastReceiveMs = millis();
        ringWrite(in, n);
        if (!bufferStarted && ringSize() >= PREBUFFER_BYTES) {
          bufferStarted = true;
          setReceiverState(RX_STREAMING);
        }
      }
    } else {
      if (!streamClient.connected()) {
        stats.lastError = "Disconnected";
        break;
      }
      if (!bufferStarted && millis() - stats.lastReceiveMs > STREAM_READ_TIMEOUT_MS) {
        stats.lastError = "Stream idle";
        setReceiverState(RX_BUFFERING, "Waiting for audio");
        stats.lastReceiveMs = millis();
      }
      vTaskDelay(1);
    }
  }
}

// HTTP Audio Stream Client Fallback
static void runHttpStream() {
  setReceiverState(RX_CONNECTING);
  if (streamClientMux) xSemaphoreTake(streamClientMux, portMAX_DELAY);
  streamClient.stop();
  bool ok = streamClient.connect(settings.phoneHost.c_str(), settings.httpPort);
  if (streamClientMux) xSemaphoreGive(streamClientMux);

  if (!ok) {
    stats.streamErrors++;
    setReceiverState(RX_ERROR, "HTTP Connect failed");
    return;
  }

  streamClient.setNoDelay(true);
  streamClient.print(String("GET /stream.wav HTTP/1.1\r\n") +
                     "Host: " + settings.phoneHost + "\r\n" +
                     "User-Agent: C3Music/1.4\r\n" +
                     "Connection: close\r\n\r\n");

  uint32_t headerStart = millis();
  bool inHeader = true;
  String line = "";

  while (streamClient.connected() && inHeader && (millis() - headerStart < HTTP_HEADER_TIMEOUT_MS)) {
    if (streamClient.available()) {
      char c = streamClient.read();
      if (c == '\r') continue;
      if (c == '\n') {
        if (line.length() == 0) inHeader = false;
        line = "";
      } else {
        line += c;
      }
    } else {
      vTaskDelay(pdMS_TO_TICKS(2));
    }
  }

  if (inHeader) {
    stats.streamErrors++;
    setReceiverState(RX_ERROR, "HTTP Header timeout");
    return;
  }

  stats.reconnects++;
  runConnectedStream();
}

static void stopStreamClient() {
  if (streamClientMux) xSemaphoreTake(streamClientMux, portMAX_DELAY);
  streamClient.stop();
  if (streamClientMux) xSemaphoreGive(streamClientMux);
}

static void streamTask(void*) {
  for (;;) {
    if (WiFi.status() != WL_CONNECTED) {
      stopStreamClient();
      ringClear();
      vTaskDelay(pdMS_TO_TICKS(500));
      continue;
    }

    if (!settings.streamEnabled || stopRequested) {
      stopStreamClient();
      ringClear();
      if (receiverState != RX_STOPPED) setReceiverState(RX_STOPPED);
      vTaskDelay(pdMS_TO_TICKS(150));
      continue;
    }

    // Direct Push Listener on port 50005
    if (tcpPushServerStarted) {
      WiFiClient pushed = tcpPushServer.available();
      if (pushed) {
        stopStreamClient();
        ringClear();
        if (streamClientMux) xSemaphoreTake(streamClientMux, portMAX_DELAY);
        streamClient = pushed;
        streamClient.setNoDelay(true);
        if (streamClientMux) xSemaphoreGive(streamClientMux);

        Serial.printf("[PUSH] Phone connected from %s\n", streamClient.remoteIP().toString().c_str());
        stats.reconnects++;
        runConnectedStream();

        stopStreamClient();
        ringClear();
        vTaskDelay(pdMS_TO_TICKS(100));
        continue;
      }
    }

    // Outbound Connect
    if (millis() - lastStreamAttemptMs < STREAM_RETRY_MS) {
      vTaskDelay(pdMS_TO_TICKS(20));
      continue;
    }
    lastStreamAttemptMs = millis();

    if (settings.preferredMode == STREAM_MODE_HTTP) {
      runHttpStream();
    } else {
      setReceiverState(RX_CONNECTING);
      bool ok = false;
      if (streamClientMux) xSemaphoreTake(streamClientMux, portMAX_DELAY);
      streamClient.stop();
      ok = streamClient.connect(settings.phoneHost.c_str(), settings.tcpPort);
      if (ok) streamClient.setNoDelay(true);
      if (streamClientMux) xSemaphoreGive(streamClientMux);

      if (ok) {
        stats.reconnects++;
        runConnectedStream();
      } else {
        if (settings.autoFallback) {
          Serial.println("[STREAM] TCP failed, trying HTTP fallback...");
          runHttpStream();
        } else {
          stats.streamErrors++;
          setReceiverState(RX_ERROR, "Phone unavailable");
        }
      }
    }

    stopStreamClient();
    ringClear();
    vTaskDelay(pdMS_TO_TICKS(STREAM_RETRY_MS));
  }
}

// Full Web Dashboard, Settings & OTA Upload
static void sendJson(int code, const String &body) {
  server.sendHeader("Cache-Control", "no-cache");
  server.send(code, "application/json", body);
}

static String makeStatusJson() {
  String r = "{";
  r += "\"device\":\"" + String(DEVICE_NAME) + "\",";
  r += "\"version\":\"" + String(FIRMWARE_VERSION) + "\",";
  r += "\"state\":\"" + String(receiverStateName(receiverState)) + "\",";
  r += "\"sampleRate\":" + String(streamFormat.sampleRate) + ",";
  r += "\"bits\":" + String(streamFormat.bitsPerSample) + ",";
  r += "\"channels\":" + String(streamFormat.channels) + ",";
  r += "\"volume\":" + String(settings.volumePercent) + ",";
  r += "\"bufferBytes\":" + String((uint32_t)ringSize()) + ",";
  r += "\"bufferPercent\":" + String(ringPercent()) + ",";
  r += "\"underruns\":" + String(stats.underruns) + ",";
  r += "\"reconnects\":" + String(stats.reconnects) + ",";
  r += "\"ip\":\"" + WiFi.localIP().toString() + "\",";
  r += "\"rssi\":" + String(WiFi.RSSI()) + ",";
  r += "\"freeHeap\":" + String(ESP.getFreeHeap());
  r += "}";
  return r;
}

static String makeConfigJson() {
  String r = "{";
  r += "\"host\":\"" + settings.phoneHost + "\",";
  r += "\"tcpPort\":" + String(settings.tcpPort) + ",";
  r += "\"httpPort\":" + String(settings.httpPort) + ",";
  r += "\"mode\":" + String((int)settings.preferredMode) + ",";
  r += "\"fallback\":" + String(settings.autoFallback ? "true" : "false") + ",";
  r += "\"autorecon\":" + String(settings.autoReconnect ? "true" : "false") + ",";
  r += "\"oled\":" + String(settings.oledEnabled ? "true" : "false") + ",";
  r += "\"enabled\":" + String(settings.streamEnabled ? "true" : "false") + ",";
  r += "\"volume\":" + String(settings.volumePercent);
  r += "}";
  return r;
}

static const char HTML_PAGE[] PROGMEM = R"HTML(
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>C3 Music Receiver</title>
<style>
body{margin:0;padding:16px;background:#121212;color:#eee;font-family:system-ui,-apple-system,sans-serif}
.container{max-width:540px;margin:0 auto}
.card{background:#1e1e1e;border-radius:12px;padding:16px;margin-bottom:16px;border:1px solid #2a2a2a}
h2{margin:0 0 12px 0;font-size:1.2rem}
.tabs{display:flex;gap:8px;margin-bottom:16px}
.tab{background:#2a2a2a;color:#aaa;padding:8px 16px;border-radius:8px;border:none;cursor:pointer;font-weight:600}
.tab.active{background:#3b82f6;color:#fff}
.row{display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid #282828}
.btn{background:#3b82f6;color:#fff;border:none;padding:10px 14px;border-radius:8px;cursor:pointer;font-weight:600}
.btn-red{background:#ef4444}
.btn-green{background:#10b981}
input,select{background:#2a2a2a;color:#fff;border:1px solid #444;padding:8px;border-radius:6px;width:100%;box-sizing:border-box;margin-top:4px}
input[type=range]{padding:0}
label{font-size:0.85rem;color:#aaa;display:block;margin-top:10px}
</style>
</head>
<body>
<div class="container">
  <div class="card" style="display:flex;justify-content:space-between;align-items:center">
    <div>
      <h2 style="margin:0">C3 Music Receiver</h2>
      <small style="color:#888">v1.4.0 • Zero-Choppy Edition</small>
    </div>
    <span id="stBadge" style="background:#2563eb;padding:4px 10px;border-radius:20px;font-size:0.8rem;font-weight:700">Connecting</span>
  </div>

  <div class="tabs">
    <button class="tab active" onclick="showTab('status')">Status</button>
    <button class="tab" onclick="showTab('controls')">Controls</button>
    <button class="tab" onclick="showTab('settings')">Settings</button>
    <button class="tab" onclick="showTab('ota')">OTA Update</button>
  </div>

  <div id="tab-status" class="card">
    <div class="row"><span>Status:</span><b id="st">—</b></div>
    <div class="row"><span>Audio Format:</span><span id="fmt">—</span></div>
    <div class="row"><span>Ring Buffer:</span><span id="buf">—</span></div>
    <div class="row"><span>Underruns:</span><span id="und">—</span></div>
    <div class="row"><span>Device IP:</span><span id="ip">—</span></div>
    <div class="row"><span>Wi-Fi Signal:</span><span id="rssi">—</span></div>
    <div style="margin-top:14px">
      <label>Hardware Volume: <span id="vval">50%</span></label>
      <input type="range" id="vol" min="0" max="100" value="50" onchange="setVol(this.value)">
    </div>
  </div>

  <div id="tab-controls" class="card" style="display:none">
    <h2>Playback Control</h2>
    <div style="display:flex;gap:10px;margin-top:12px">
      <button class="btn btn-green" style="flex:1" onclick="fetch('/api/stream/start',{method:'POST'}).then(update)">Start</button>
      <button class="btn btn-red" style="flex:1" onclick="fetch('/api/stream/stop',{method:'POST'}).then(update)">Stop</button>
      <button class="btn" style="flex:1" onclick="fetch('/api/stream/reconnect',{method:'POST'}).then(update)">Reconnect</button>
    </div>
    <div style="margin-top:16px">
      <button class="btn btn-red" style="width:100%" onclick="if(confirm('Reboot C3 Receiver?')) fetch('/api/system/reboot',{method:'POST'})">Reboot Device</button>
    </div>
  </div>

  <div id="tab-settings" class="card" style="display:none">
    <h2>Receiver Configuration</h2>
    <label>Phone IP Address</label>
    <input type="text" id="cfgHost">
    <label>TCP Streaming Port</label>
    <input type="number" id="cfgTcp">
    <label>HTTP Port</label>
    <input type="number" id="cfgHttp">
    <label>Preferred Mode</label>
    <select id="cfgMode">
      <option value="0">TCP Socket (Recommended)</option>
      <option value="1">HTTP Stream</option>
    </select>
    <div style="margin-top:16px">
      <button class="btn" style="width:100%" onclick="saveCfg()">Save Settings</button>
    </div>
  </div>

  <div id="tab-ota" class="card" style="display:none">
    <h2>Firmware Update (.bin)</h2>
    <p style="font-size:0.85rem;color:#aaa">Select your compiled firmware.bin file to flash over Wi-Fi:</p>
    <form method="POST" action="/api/ota" enctype="multipart/form-data" id="otaForm">
      <input type="file" name="update" id="otaFile" style="margin-bottom:12px" accept=".bin">
      <button type="submit" class="btn btn-green" style="width:100%">Upload & Flash Firmware</button>
    </form>
    <div id="otaProgress" style="display:none;margin-top:12px;font-weight:bold;color:#3b82f6">Flashing... please wait...</div>
  </div>
</div>

<script>
function showTab(t){
  ['status','controls','settings','ota'].forEach(id=>{
    document.getElementById('tab-'+id).style.display = (id===t)?'block':'none';
  });
  document.querySelectorAll('.tab').forEach((el,i)=>{
    el.classList.toggle('active', ['status','controls','settings','ota'][i]===t);
  });
  if(t==='settings') loadCfg();
}

function update(){
  fetch('/api/status').then(r=>r.json()).then(d=>{
    document.getElementById('st').textContent=d.state;
    document.getElementById('stBadge').textContent=d.state;
    document.getElementById('fmt').textContent=d.sampleRate+' Hz • '+d.bits+'-bit • '+(d.channels==2?'Stereo':'Mono');
    document.getElementById('buf').textContent=d.bufferPercent+'% ('+d.bufferBytes+' bytes)';
    document.getElementById('und').textContent=d.underruns;
    document.getElementById('ip').textContent=d.ip;
    document.getElementById('rssi').textContent=d.rssi+' dBm';
  }).catch(()=>{});
}

function setVol(v){
  document.getElementById('vval').textContent=v+'%';
  fetch('/api/volume',{method:'POST',body:new URLSearchParams({value:v})});
}

function loadCfg(){
  fetch('/api/config').then(r=>r.json()).then(d=>{
    document.getElementById('cfgHost').value=d.host;
    document.getElementById('cfgTcp').value=d.tcpPort;
    document.getElementById('cfgHttp').value=d.httpPort;
    document.getElementById('cfgMode').value=d.mode;
  });
}

function saveCfg(){
  const body = new URLSearchParams({
    host: document.getElementById('cfgHost').value,
    tcpPort: document.getElementById('cfgTcp').value,
    httpPort: document.getElementById('cfgHttp').value,
    mode: document.getElementById('cfgMode').value
  });
  fetch('/api/config',{method:'POST',body}).then(()=>alert('Settings Saved!'));
}

document.getElementById('otaForm').onsubmit = function(){
  document.getElementById('otaProgress').style.display='block';
};

setInterval(update, 1000);
update();
</script>
</body>
</html>
)HTML";

static void setupWebServer() {
  server.on("/", HTTP_GET, []() {
    server.send(200, "text/html", HTML_PAGE);
  });
  server.on("/api/status", HTTP_GET, []() {
    sendJson(200, makeStatusJson());
  });
  server.on("/api/config", HTTP_GET, []() {
    sendJson(200, makeConfigJson());
  });
  server.on("/api/config", HTTP_POST, []() {
    if (server.hasArg("host")) settings.phoneHost = server.arg("host");
    if (server.hasArg("tcpPort")) settings.tcpPort = server.arg("tcpPort").toInt();
    if (server.hasArg("httpPort")) settings.httpPort = server.arg("httpPort").toInt();
    if (server.hasArg("mode")) settings.preferredMode = (StreamMode)server.arg("mode").toInt();
    saveSettings();
    sendJson(200, "{\"ok\":true}");
  });
  server.on("/api/volume", HTTP_POST, []() {
    int v = server.arg("value").toInt();
    if (v >= 0 && v <= 100) {
      settings.volumePercent = (uint8_t)v;
      preferences.putUChar("volume", settings.volumePercent);
    }
    sendJson(200, "{\"ok\":true}");
  });
  server.on("/api/stream/start", HTTP_POST, []() {
    settings.streamEnabled = true;
    stopRequested = false;
    sendJson(200, "{\"ok\":true}");
  });
  server.on("/api/stream/stop", HTTP_POST, []() {
    settings.streamEnabled = false;
    stopRequested = true;
    stopStreamClient();
    ringClear();
    setReceiverState(RX_STOPPED);
    sendJson(200, "{\"ok\":true}");
  });
  server.on("/api/stream/reconnect", HTTP_POST, []() {
    stopStreamClient();
    ringClear();
    lastStreamAttemptMs = 0;
    sendJson(200, "{\"ok\":true}");
  });
  server.on("/api/system/reboot", HTTP_POST, []() {
    sendJson(200, "{\"ok\":true}");
    delay(200);
    ESP.restart();
  });

  // Browser OTA Upload Handler
  server.on("/api/ota", HTTP_POST, []() {
    server.sendHeader("Connection", "close");
    server.send(200, "text/plain", (Update.hasError()) ? "OTA FAILED" : "OTA SUCCESS! Rebooting...");
    delay(500);
    ESP.restart();
  }, []() {
    HTTPUpload& upload = server.upload();
    if (upload.status == UPLOAD_FILE_START) {
      setReceiverState(RX_UPDATING);
      stopRequested = true;
      stopStreamClient();
      ringClear();
      Serial.printf("[OTA] Starting: %s\n", upload.filename.c_str());
      if (!Update.begin(UPDATE_SIZE_UNKNOWN)) {
        Update.printError(Serial);
      }
    } else if (upload.status == UPLOAD_FILE_WRITE) {
      if (Update.write(upload.buf, upload.currentSize) != upload.currentSize) {
        Update.printError(Serial);
      }
    } else if (upload.status == UPLOAD_FILE_END) {
      if (Update.end(true)) {
        Serial.printf("[OTA] Success: %u bytes\n", upload.totalSize);
      } else {
        Update.printError(Serial);
      }
    }
  });

  server.begin();
}

void setup() {
  Serial.begin(115200);
  delay(300);
  Serial.println("\n--- C3 High-Performance Music Receiver v1.4.0 ---");

  loadSettings();
  audioDataSemaphore = xSemaphoreCreateBinary();
  i2sMux = xSemaphoreCreateMutex();
  streamClientMux = xSemaphoreCreateMutex();

  displayBegin();
  setReceiverState(RX_BOOTING);

  WiFi.persistent(false);
  WiFi.mode(WIFI_STA);
  WiFi.setSleep(false);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);

  setupWebServer();

  xTaskCreate(playbackTask, "playback", 8192, nullptr, 3, &playbackTaskHandle);
  xTaskCreate(streamTask, "stream", 6144, nullptr, 2, &streamTaskHandle);

  displayUpdate();
}

void loop() {
  server.handleClient();

  if (WiFi.status() == WL_CONNECTED) {
    if (!mdnsStarted) {
      if (MDNS.begin(MDNS_HOSTNAME)) {
        MDNS.addService("http", "tcp", 80);
        MDNS.addService("c3stream", "tcp", TCP_DEFAULT_PORT);
        mdnsStarted = true;
      }
    }
    if (!tcpPushServerStarted) {
      tcpPushServer.begin();
      tcpPushServer.setNoDelay(true);
      tcpPushServerStarted = true;
      Serial.printf("[PUSH SERVER] Listening on port %u\n", TCP_DEFAULT_PORT);
    }
  }

  displayUpdate();
  delay(2);
}
