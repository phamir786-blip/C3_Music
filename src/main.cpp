/*
  C3 Music Receiver
  ESP32-C3 + embedded SSD1306 72x40 OLED + UDA1334A I2S DAC

  Primary input: raw TCP PCM from PHONE_HOST:50005
  Fallback input: HTTP/WAV PCM from http://PHONE_HOST:8080/
  Output: I2S PCM to UDA1334A

  GPIO map:
    OLED I2C: SDA GPIO5, SCL GPIO6
    UDA1334A: BCLK GPIO4, LRCLK/WSEL GPIO7, DIN GPIO10
*/

#include <Arduino.h>
#include <WiFi.h>
#include <WiFiClient.h>
#include <WebServer.h>
#include <ESPmDNS.h>
#include <Preferences.h>
#include <Update.h>
#include <U8g2lib.h>
#include <driver/i2s.h>

// ============================================================================
// 01. EDIT ONLY THESE VALUES BEFORE FIRST FLASH
// ============================================================================

static const char WIFI_SSID[] = "GFiber_2.4_Coverage_AECD9";
static const char WIFI_PASSWORD[] = "006BF4FD";
static const char PHONE_HOST[] = "192.168.1.100";

// ============================================================================
// 02. DEVICE / HARDWARE CONFIGURATION
// ============================================================================

static const char DEVICE_NAME[] = "C3 Music Receiver";
static const char MDNS_HOSTNAME[] = "c3music";
static const char FIRMWARE_VERSION[] = "1.0.0";

static constexpr uint8_t OLED_SDA = 5;
static constexpr uint8_t OLED_SCL = 6;
static constexpr uint8_t OLED_WIDTH = 72;
static constexpr uint8_t OLED_HEIGHT = 40;

static constexpr int I2S_BCLK_PIN = 4;
static constexpr int I2S_LRCLK_PIN = 7;
static constexpr int I2S_DOUT_PIN = 10;

static constexpr uint16_t TCP_DEFAULT_PORT = 50005;
static constexpr uint16_t HTTP_DEFAULT_PORT = 8080;

static constexpr uint32_t DEFAULT_SAMPLE_RATE = 44100;
static constexpr uint8_t DEFAULT_CHANNELS = 2;
static constexpr uint8_t DEFAULT_BITS_PER_SAMPLE = 16;

static constexpr size_t AUDIO_RING_BYTES = 65536;
static constexpr size_t NETWORK_READ_BYTES = 1460;
static constexpr size_t I2S_WRITE_BYTES = 2048;
static constexpr uint32_t PREBUFFER_BYTES = 22050;
static constexpr uint32_t STREAM_RETRY_MS = 2500;
static constexpr uint32_t WIFI_RETRY_MS = 10000;
static constexpr uint32_t OLED_REFRESH_MS = 650;
static constexpr uint32_t STATUS_REFRESH_MS = 1000;

static constexpr uint32_t HTTP_HEADER_TIMEOUT_MS = 8000;
static constexpr uint32_t STREAM_READ_TIMEOUT_MS = 3500;
static constexpr uint32_t WAV_PARSE_MAX_BYTES = 4096;

// ============================================================================
// 03. TYPES / GLOBAL STATE
// ============================================================================

enum StreamMode : uint8_t {
  STREAM_MODE_TCP = 0,
  STREAM_MODE_HTTP = 1
};

enum ReceiverState : uint8_t {
  RX_BOOTING = 0,
  RX_WIFI_CONNECTING,
  RX_WIFI_OFFLINE,
  RX_IDLE,
  RX_CONNECTING,
  RX_BUFFERING,
  RX_STREAMING,
  RX_STOPPED,
  RX_ERROR,
  RX_UPDATING
};

struct StreamFormat {
  uint32_t sampleRate = DEFAULT_SAMPLE_RATE;
  uint16_t channels = DEFAULT_CHANNELS;
  uint16_t bitsPerSample = DEFAULT_BITS_PER_SAMPLE;
  uint16_t audioFormat = 1;
  bool valid = false;
};

struct RuntimeStats {
  uint32_t reconnects = 0;
  uint32_t underruns = 0;
  uint32_t streamErrors = 0;
  uint32_t bytesReceived = 0;
  uint32_t bytesPlayed = 0;
  uint32_t sessionStartedMs = 0;
  uint32_t lastReceiveMs = 0;
  int lastHttpStatus = 0;
  String lastError;
};

struct Settings {
  String phoneHost;
  uint16_t tcpPort = TCP_DEFAULT_PORT;
  uint16_t httpPort = HTTP_DEFAULT_PORT;
  StreamMode preferredMode = STREAM_MODE_TCP;
  bool autoFallback = true;
  bool autoReconnect = true;
  bool oledEnabled = true;
  bool streamEnabled = true;
  uint16_t targetBufferMs = 250;
};

static U8G2_SSD1306_72X40_ER_F_HW_I2C oled(U8G2_R0, U8X8_PIN_NONE);
static bool oledAvailable = false;

static Preferences preferences;
static WebServer server(80);
static WiFiClient streamClient;

static Settings settings;
static StreamFormat streamFormat;
static RuntimeStats stats;

static volatile ReceiverState receiverState = RX_BOOTING;
static volatile bool stopRequested = false;
static volatile bool streamTaskRunning = false;
static volatile bool i2sReady = false;
static volatile bool bufferStarted = false;

static uint8_t audioRing[AUDIO_RING_BYTES];
static volatile size_t ringReadIndex = 0;
static volatile size_t ringWriteIndex = 0;
static volatile size_t ringCount = 0;

static portMUX_TYPE ringMux = portMUX_INITIALIZER_UNLOCKED;
static SemaphoreHandle_t audioDataSemaphore = nullptr;
static TaskHandle_t streamTaskHandle = nullptr;
static TaskHandle_t playbackTaskHandle = nullptr;

static i2s_chan_handle_t i2sTxHandle = nullptr;

static uint32_t lastWifiAttemptMs = 0;
static uint32_t lastStreamAttemptMs = 0;
static uint32_t lastOledRefreshMs = 0;
static uint32_t lastStatusRefreshMs = 0;
static bool mdnsStarted = false;

// ============================================================================
// 04. FORWARD DECLARATIONS
// ============================================================================

static void loadSettings();
static void saveSettings();
static void resetSettings();

static void setReceiverState(ReceiverState value, const String &error = "");
static const char *receiverStateName(ReceiverState value);
static const char *streamModeName(StreamMode mode);

static bool ringWrite(const uint8_t *data, size_t length);
static size_t ringRead(uint8_t *output, size_t maxLength);
static void ringClear();
static size_t ringSize();
static uint8_t ringPercent();

static void displayBegin();
static void displayUpdate();
static void displayLineCenter(const char *text, uint8_t y, const uint8_t *font);

static void connectWifiIfNeeded();
static void beginMdnsIfNeeded();

static bool beginI2S(uint32_t sampleRate, uint16_t channels, uint16_t bitsPerSample);
static void endI2S();
static bool supportedPcmFormat(const StreamFormat &format);

static bool connectRawTcp();
static bool connectHttpWav();
static bool parseHttpHeaders();
static bool parseWavHeader(StreamFormat &format);

static bool readExact(WiFiClient &client, uint8_t *buffer, size_t length, uint32_t timeoutMs);
static bool readLine(WiFiClient &client, String &line, uint32_t timeoutMs);

static void streamTask(void *parameter);
static void playbackTask(void *parameter);

static void startStreaming();
static void stopStreaming();
static void reconnectStreaming();

static String jsonEscape(const String &input);
static String makeStatusJson();
static String makeConfigJson();
static void sendJson(int statusCode, const String &body);
static bool requirePost();
static void setupWebServer();
static void handleFirmwareUpload();

static String htmlPage();

// ============================================================================
// 05. SETTINGS / NVS
// ============================================================================

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
  settings.targetBufferMs = preferences.getUShort("buffer", 250);

  if (settings.phoneHost.length() == 0) {
    settings.phoneHost = PHONE_HOST;
  }

  if (settings.tcpPort == 0) {
    settings.tcpPort = TCP_DEFAULT_PORT;
  }

  if (settings.httpPort == 0) {
    settings.httpPort = HTTP_DEFAULT_PORT;
  }

  if (settings.targetBufferMs < 80) {
    settings.targetBufferMs = 80;
  }

  if (settings.targetBufferMs > 700) {
    settings.targetBufferMs = 700;
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
  preferences.putUShort("buffer", settings.targetBufferMs);
}

static void resetSettings() {
  preferences.clear();
  preferences.end();

  settings.phoneHost = PHONE_HOST;
  settings.tcpPort = TCP_DEFAULT_PORT;
  settings.httpPort = HTTP_DEFAULT_PORT;
  settings.preferredMode = STREAM_MODE_TCP;
  settings.autoFallback = true;
  settings.autoReconnect = true;
  settings.oledEnabled = true;
  settings.streamEnabled = true;
  settings.targetBufferMs = 250;

  preferences.begin("c3music", false);
  saveSettings();
}

// ============================================================================
// 06. STATE / STATUS
// ============================================================================

static void setReceiverState(ReceiverState value, const String &error) {
  receiverState = value;

  if (error.length() > 0) {
    stats.lastError = error;
    Serial.printf("[STATE] %s: %s
", receiverStateName(value), error.c_str());
  } else {
    Serial.printf("[STATE] %s
", receiverStateName(value));
  }
}

static const char *receiverStateName(ReceiverState value) {
  switch (value) {
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

static const char *streamModeName(StreamMode mode) {
  return mode == STREAM_MODE_HTTP ? "HTTP WAV" : "TCP PCM";
}

// ============================================================================
// 07. PCM RING BUFFER
// ============================================================================

static bool ringWrite(const uint8_t *data, size_t length) {
  if (data == nullptr || length == 0) {
    return true;
  }

  bool allWritten = true;

  portENTER_CRITICAL(&ringMux);

  size_t freeBytes = AUDIO_RING_BYTES - ringCount;

  if (length > freeBytes) {
    length = freeBytes;
    allWritten = false;
  }

  size_t firstPart = min(length, AUDIO_RING_BYTES - ringWriteIndex);
  memcpy(audioRing + ringWriteIndex, data, firstPart);

  size_t secondPart = length - firstPart;
  if (secondPart > 0) {
    memcpy(audioRing, data + firstPart, secondPart);
  }

  ringWriteIndex = (ringWriteIndex + length) % AUDIO_RING_BYTES;
  ringCount += length;

  portEXIT_CRITICAL(&ringMux);

  if (length > 0 && audioDataSemaphore != nullptr) {
    xSemaphoreGive(audioDataSemaphore);
  }

  return allWritten;
}

static size_t ringRead(uint8_t *output, size_t maxLength) {
  if (output == nullptr || maxLength == 0) {
    return 0;
  }

  portENTER_CRITICAL(&ringMux);

  size_t length = min(maxLength, ringCount);

  if (length > 0) {
    size_t firstPart = min(length, AUDIO_RING_BYTES - ringReadIndex);
    memcpy(output, audioRing + ringReadIndex, firstPart);

    size_t secondPart = length - firstPart;
    if (secondPart > 0) {
      memcpy(output + firstPart, audioRing, secondPart);
    }

    ringReadIndex = (ringReadIndex + length) % AUDIO_RING_BYTES;
    ringCount -= length;
  }

  portEXIT_CRITICAL(&ringMux);

  return length;
}

static void ringClear() {
  portENTER_CRITICAL(&ringMux);
  ringReadIndex = 0;
  ringWriteIndex = 0;
  ringCount = 0;
  portEXIT_CRITICAL(&ringMux);

  bufferStarted = false;
}

static size_t ringSize() {
  portENTER_CRITICAL(&ringMux);
  size_t value = ringCount;
  portEXIT_CRITICAL(&ringMux);
  return value;
}

static uint8_t ringPercent() {
  size_t value = ringSize();
  return (uint8_t)((value * 100UL) / AUDIO_RING_BYTES);
}

// ============================================================================
// 08. OLED
// ============================================================================

static void displayBegin() {
  Wire.begin(OLED_SDA, OLED_SCL);
  oled.begin();
  oled.setPowerSave(settings.oledEnabled ? 0 : 1);
  oled.clearBuffer();
  oled.setFont(u8g2_font_5x7_tf);
  oled.drawStr(0, 7, "C3 MUSIC");
  oled.drawStr(0, 17, "Starting...");
  oled.sendBuffer();
  oledAvailable = true;
}

static void displayLineCenter(const char *text, uint8_t y, const uint8_t *font) {
  oled.setFont(font);
  int width = oled.getStrWidth(text);
  int x = (OLED_WIDTH - width) / 2;
  if (x < 0) {
    x = 0;
  }
  oled.drawStr(x, y, text);
}

static void displayUpdate() {
  if (!oledAvailable || !settings.oledEnabled) {
    return;
  }

  if (millis() - lastOledRefreshMs < OLED_REFRESH_MS) {
    return;
  }

  lastOledRefreshMs = millis();

  char line[32];
  oled.clearBuffer();
  oled.setFont(u8g2_font_5x7_tf);
  oled.drawStr(0, 7, "C3 MUSIC");

  const char *shortState = "BOOT";
  switch (receiverState) {
    case RX_WIFI_CONNECTING: shortState = "WIFI"; break;
    case RX_WIFI_OFFLINE: shortState = "NO WIFI"; break;
    case RX_IDLE: shortState = "READY"; break;
    case RX_CONNECTING: shortState = "LINK"; break;
    case RX_BUFFERING: shortState = "BUFFER"; break;
    case RX_STREAMING: shortState = "PLAY"; break;
    case RX_STOPPED: shortState = "STOP"; break;
    case RX_ERROR: shortState = "ERROR"; break;
    case RX_UPDATING: shortState = "OTA"; break;
    default: break;
  }

  int stateWidth = oled.getStrWidth(shortState);
  oled.drawStr(OLED_WIDTH - stateWidth, 7, shortState);

  if (receiverState == RX_STREAMING || receiverState == RX_BUFFERING) {
    snprintf(line, sizeof(line), "%luk %s",
             (unsigned long)(streamFormat.sampleRate / 1000),
             streamFormat.channels == 2 ? "ST" : "MO");
    displayLineCenter(line, 19, u8g2_font_6x10_tf);

    snprintf(line, sizeof(line), "BUF %u%%", ringPercent());
    displayLineCenter(line, 29, u8g2_font_4x6_tf);

    oled.drawFrame(0, 33, OLED_WIDTH, 7);
    uint8_t width = (uint8_t)(((OLED_WIDTH - 2) * ringPercent()) / 100);
    if (width > 0) {
      oled.drawBox(1, 34, width, 5);
    }
  } else {
    String message;

    if (WiFi.status() == WL_CONNECTED) {
      message = WiFi.localIP().toString();
    } else if (receiverState == RX_WIFI_CONNECTING) {
      message = "Connecting...";
    } else {
      message = "Check WiFi";
    }

    displayLineCenter(message.c_str(), 22, u8g2_font_6x10_tf);

    if (receiverState == RX_ERROR && stats.lastError.length() > 0) {
      String error = stats.lastError;
      if (error.length() > 17) {
        error = error.substring(0, 17);
      }
      displayLineCenter(error.c_str(), 38, u8g2_font_4x6_tf);
    } else {
      displayLineCenter("c3music.local", 38, u8g2_font_4x6_tf);
    }
  }

  oled.sendBuffer();
}

// ============================================================================
// 09. WI-FI / MDNS
// ============================================================================

static void connectWifiIfNeeded() {
  if (WiFi.status() == WL_CONNECTED) {
    beginMdnsIfNeeded();
    return;
  }

  if (millis() - lastWifiAttemptMs < WIFI_RETRY_MS) {
    return;
  }

  lastWifiAttemptMs = millis();
  setReceiverState(RX_WIFI_CONNECTING);

  WiFi.mode(WIFI_STA);
  WiFi.setHostname(MDNS_HOSTNAME);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);

  Serial.printf("[WIFI] Connecting to %s
", WIFI_SSID);
}

static void beginMdnsIfNeeded() {
  if (mdnsStarted || WiFi.status() != WL_CONNECTED) {
    return;
  }

  if (MDNS.begin(MDNS_HOSTNAME)) {
    MDNS.addService("http", "tcp", 80);
    mdnsStarted = true;
    Serial.printf("[MDNS] http://%s.local/
", MDNS_HOSTNAME);
  } else {
    Serial.println("[MDNS] Failed to start");
  }
}

// ============================================================================
// 10. I2S / UDA1334A
// ============================================================================

static bool supportedPcmFormat(const StreamFormat &format) {
  if (format.audioFormat != 1) {
    return false;
  }

  if (format.bitsPerSample != 16) {
    return false;
  }

  if (format.channels != 1 && format.channels != 2) {
    return false;
  }

  if (format.sampleRate < 8000 || format.sampleRate > 96000) {
    return false;
  }

  return true;
}

static void endI2S() {
  if (i2sTxHandle != nullptr) {
    i2s_channel_disable(i2sTxHandle);
    i2s_del_channel(i2sTxHandle);
    i2sTxHandle = nullptr;
  }

  i2sReady = false;
}

static bool beginI2S(uint32_t sampleRate, uint16_t channels, uint16_t bitsPerSample) {
  if (bitsPerSample != 16 || (channels != 1 && channels != 2)) {
    return false;
  }

  endI2S();

  i2s_chan_config_t channelConfig = I2S_CHANNEL_DEFAULT_CONFIG(I2S_NUM_AUTO, I2S_ROLE_MASTER);

  if (i2s_new_channel(&channelConfig, &i2sTxHandle, nullptr) != ESP_OK) {
    i2sTxHandle = nullptr;
    return false;
  }

  i2s_std_config_t stdConfig = {
    .clk_cfg = I2S_STD_CLK_DEFAULT_CONFIG(sampleRate),
    .slot_cfg = I2S_STD_PHILIPS_SLOT_DEFAULT_CONFIG(
      I2S_DATA_BIT_WIDTH_16BIT,
      channels == 2 ? I2S_SLOT_MODE_STEREO : I2S_SLOT_MODE_MONO
    ),
    .gpio_cfg = {
      .mclk = I2S_GPIO_UNUSED,
      .bclk = (gpio_num_t)I2S_BCLK_PIN,
      .ws = (gpio_num_t)I2S_LRCLK_PIN,
      .dout = (gpio_num_t)I2S_DOUT_PIN,
      .din = I2S_GPIO_UNUSED,
      .invert_flags = {
        .mclk_inv = false,
        .bclk_inv = false,
        .ws_inv = false
      }
    }
  };

  if (i2s_channel_init_std_mode(i2sTxHandle, &stdConfig) != ESP_OK) {
    endI2S();
    return false;
  }

  if (i2s_channel_enable(i2sTxHandle) != ESP_OK) {
    endI2S();
    return false;
  }

  i2sReady = true;

  Serial.printf("[I2S] %lu Hz, %u-bit, %u channel(s), BCLK=%d WS=%d DOUT=%d
",
                (unsigned long)sampleRate,
                bitsPerSample,
                channels,
                I2S_BCLK_PIN,
                I2S_LRCLK_PIN,
                I2S_DOUT_PIN);

  return true;
}

// ============================================================================
// 11. STREAM TRANSPORT / WAV PARSING
// ============================================================================

static bool readExact(WiFiClient &client, uint8_t *buffer, size_t length, uint32_t timeoutMs) {
  size_t received = 0;
  uint32_t started = millis();

  while (received < length && !stopRequested) {
    int available = client.available();

    if (available > 0) {
      size_t wanted = min((size_t)available, length - received);
      int count = client.read(buffer + received, wanted);

      if (count > 0) {
        received += (size_t)count;
        started = millis();
      }
    } else {
      if (!client.connected() || millis() - started > timeoutMs) {
        return false;
      }
      delay(1);
    }
  }

  return received == length;
}

static bool readLine(WiFiClient &client, String &line, uint32_t timeoutMs) {
  line = "";
  uint32_t started = millis();

  while (!stopRequested) {
    if (client.available()) {
      char c = (char)client.read();
      line += c;

      if (line.endsWith("
")) {
        return true;
      }

      if (line.length() > 1024) {
        return false;
      }

      started = millis();
    } else {
      if (!client.connected() || millis() - started > timeoutMs) {
        return false;
      }
      delay(1);
    }
  }

  return false;
}

static bool parseHttpHeaders() {
  String line;

  if (!readLine(streamClient, line, HTTP_HEADER_TIMEOUT_MS)) {
    stats.lastError = "HTTP no status";
    return false;
  }

  line.trim();
  Serial.printf("[HTTP] %s
", line.c_str());

  if (!line.startsWith("HTTP/") || line.indexOf(" 200") < 0) {
    stats.lastError = "HTTP status " + line;
    return false;
  }

  stats.lastHttpStatus = 200;

  while (!stopRequested) {
    if (!readLine(streamClient, line, HTTP_HEADER_TIMEOUT_MS)) {
      stats.lastError = "HTTP header timeout";
      return false;
    }

    if (line == "
") {
      return true;
    }

    line.trim();

    if (line.length() > 0) {
      Serial.printf("[HTTP] %s
", line.c_str());
    }
  }

  return false;
}

static uint32_t readLe32(const uint8_t *p) {
  return ((uint32_t)p[0]) |
         ((uint32_t)p[1] << 8) |
         ((uint32_t)p[2] << 16) |
         ((uint32_t)p[3] << 24);
}

static uint16_t readLe16(const uint8_t *p) {
  return ((uint16_t)p[0]) | ((uint16_t)p[1] << 8);
}

static bool parseWavHeader(StreamFormat &format) {
  uint8_t riff[12];

  if (!readExact(streamClient, riff, sizeof(riff), HTTP_HEADER_TIMEOUT_MS)) {
    stats.lastError = "WAV header missing";
    return false;
  }

  if (memcmp(riff, "RIFF", 4) != 0 || memcmp(riff + 8, "WAVE", 4) != 0) {
    stats.lastError = "Not RIFF/WAV";
    return false;
  }

  bool foundFormat = false;
  bool foundData = false;
  size_t scanned = 12;

  while (!stopRequested && scanned < WAV_PARSE_MAX_BYTES) {
    uint8_t chunkHeader[8];

    if (!readExact(streamClient, chunkHeader, sizeof(chunkHeader), HTTP_HEADER_TIMEOUT_MS)) {
      stats.lastError = "WAV chunk timeout";
      return false;
    }

    scanned += sizeof(chunkHeader);

    uint32_t chunkSize = readLe32(chunkHeader + 4);
    char chunkName[5] = {
      (char)chunkHeader[0],
      (char)chunkHeader[1],
      (char)chunkHeader[2],
      (char)chunkHeader[3],
      ''
    };

    if (memcmp(chunkHeader, "fmt ", 4) == 0) {
      if (chunkSize < 16 || chunkSize > 64) {
        stats.lastError = "Bad WAV fmt";
        return false;
      }

      uint8_t fmt[64];
      if (!readExact(streamClient, fmt, chunkSize, HTTP_HEADER_TIMEOUT_MS)) {
        stats.lastError = "WAV fmt timeout";
        return false;
      }

      scanned += chunkSize;

      format.audioFormat = readLe16(fmt + 0);
      format.channels = readLe16(fmt + 2);
      format.sampleRate = readLe32(fmt + 4);
      format.bitsPerSample = readLe16(fmt + 14);
      format.valid = supportedPcmFormat(format);

      Serial.printf("[WAV] fmt=%u channels=%u rate=%lu bits=%u
",
                    format.audioFormat,
                    format.channels,
                    (unsigned long)format.sampleRate,
                    format.bitsPerSample);

      if (!format.valid) {
        stats.lastError = "Unsupported WAV PCM";
        return false;
      }

      foundFormat = true;
    } else if (memcmp(chunkHeader, "data", 4) == 0) {
      foundData = true;
      Serial.printf("[WAV] data chunk (%lu bytes, stream may be indefinite)
",
                    (unsigned long)chunkSize);
      break;
    } else {
      Serial.printf("[WAV] Skip %.4s (%lu)
", chunkName, (unsigned long)chunkSize);

      uint8_t discard[128];
      uint32_t remaining = chunkSize;

      while (remaining > 0) {
        size_t part = min((uint32_t)sizeof(discard), remaining);

        if (!readExact(streamClient, discard, part, HTTP_HEADER_TIMEOUT_MS)) {
          stats.lastError = "WAV skip timeout";
          return false;
        }

        remaining -= part;
        scanned += part;
      }
    }

    if (chunkSize & 1U) {
      uint8_t pad;
      if (!readExact(streamClient, &pad, 1, HTTP_HEADER_TIMEOUT_MS)) {
        stats.lastError = "WAV pad timeout";
        return false;
      }
      scanned++;
    }
  }

  if (!foundFormat || !foundData) {
    stats.lastError = "WAV chunks invalid";
    return false;
  }

  return true;
}

static bool connectRawTcp() {
  setReceiverState(RX_CONNECTING);
  streamFormat.sampleRate = DEFAULT_SAMPLE_RATE;
  streamFormat.channels = DEFAULT_CHANNELS;
  streamFormat.bitsPerSample = DEFAULT_BITS_PER_SAMPLE;
  streamFormat.audioFormat = 1;
  streamFormat.valid = true;

  Serial.printf("[TCP] Connecting to %s:%u
", settings.phoneHost.c_str(), settings.tcpPort);

  if (!streamClient.connect(settings.phoneHost.c_str(), settings.tcpPort)) {
    stats.lastError = "TCP host unavailable";
    return false;
  }

  streamClient.setNoDelay(true);
  streamClient.setTimeout(1);

  if (!beginI2S(streamFormat.sampleRate, streamFormat.channels, streamFormat.bitsPerSample)) {
    stats.lastError = "I2S setup failed";
    streamClient.stop();
    return false;
  }

  return true;
}

static bool connectHttpWav() {
  setReceiverState(RX_CONNECTING);
  stats.lastHttpStatus = 0;

  Serial.printf("[HTTP] Connecting to http://%s:%u/
",
                settings.phoneHost.c_str(),
                settings.httpPort);

  if (!streamClient.connect(settings.phoneHost.c_str(), settings.httpPort)) {
    stats.lastError = "HTTP host unavailable";
    return false;
  }

  streamClient.setNoDelay(true);
  streamClient.setTimeout(1);

  streamClient.printf(
    "GET / HTTP/1.1
"
    "Host: %s:%u
"
    "User-Agent: C3MusicReceiver/%s
"
    "Accept: audio/wav,audio/x-wav,application/octet-stream,*/*
"
    "Connection: keep-alive
"
    "
",
    settings.phoneHost.c_str(),
    settings.httpPort,
    FIRMWARE_VERSION
  );

  if (!parseHttpHeaders()) {
    streamClient.stop();
    return false;
  }

  StreamFormat parsedFormat;

  if (!parseWavHeader(parsedFormat)) {
    streamClient.stop();
    return false;
  }

  streamFormat = parsedFormat;

  if (!beginI2S(streamFormat.sampleRate, streamFormat.channels, streamFormat.bitsPerSample)) {
    stats.lastError = "I2S setup failed";
    streamClient.stop();
    return false;
  }

  return true;
}

// ============================================================================
// 12. AUDIO / STREAM TASKS
// ============================================================================

static void playbackTask(void *parameter) {
  uint8_t output[I2S_WRITE_BYTES];

  for (;;) {
    if (!i2sReady || i2sTxHandle == nullptr || !bufferStarted) {
      vTaskDelay(pdMS_TO_TICKS(10));
      continue;
    }

    size_t count = ringRead(output, sizeof(output));

    if (count == 0) {
      stats.underruns++;
      bufferStarted = false;

      if (receiverState == RX_STREAMING) {
        setReceiverState(RX_BUFFERING, "Audio buffer low");
      }

      vTaskDelay(pdMS_TO_TICKS(4));
      continue;
    }

    size_t written = 0;
    esp_err_t result = i2s_channel_write(
      i2sTxHandle,
      output,
      count,
      &written,
      pdMS_TO_TICKS(80)
    );

    if (result == ESP_OK && written > 0) {
      stats.bytesPlayed += written;
    } else {
      vTaskDelay(pdMS_TO_TICKS(2));
    }
  }
}

static void runConnectedStream() {
  uint8_t incoming[NETWORK_READ_BYTES];
  stats.sessionStartedMs = millis();
  stats.lastReceiveMs = millis();
  ringClear();
  setReceiverState(RX_BUFFERING);

  while (!stopRequested && streamClient.connected() && WiFi.status() == WL_CONNECTED) {
    int available = streamClient.available();

    if (available > 0) {
      size_t wanted = min((size_t)available, sizeof(incoming));
      int count = streamClient.read(incoming, wanted);

      if (count > 0) {
        stats.bytesReceived += (uint32_t)count;
        stats.lastReceiveMs = millis();

        if (!ringWrite(incoming, (size_t)count)) {
          vTaskDelay(pdMS_TO_TICKS(3));
        }

        if (!bufferStarted && ringSize() >= PREBUFFER_BYTES) {
          bufferStarted = true;
          setReceiverState(RX_STREAMING);
        }
      }
    } else {
      if (millis() - stats.lastReceiveMs > STREAM_READ_TIMEOUT_MS) {
        stats.lastError = "Stream timeout";
        break;
      }

      vTaskDelay(pdMS_TO_TICKS(1));
    }
  }
}

static void streamTask(void *parameter) {
  for (;;) {
    if (WiFi.status() != WL_CONNECTED) {
      streamClient.stop();
      endI2S();
      ringClear();

      if (settings.streamEnabled && !stopRequested) {
        setReceiverState(RX_WIFI_OFFLINE, "WiFi disconnected");
      }

      vTaskDelay(pdMS_TO_TICKS(500));
      continue;
    }

    if (!settings.streamEnabled || stopRequested) {
      streamClient.stop();
      endI2S();
      ringClear();

      if (receiverState != RX_STOPPED) {
        setReceiverState(RX_STOPPED);
      }

      vTaskDelay(pdMS_TO_TICKS(150));
      continue;
    }

    if (millis() - lastStreamAttemptMs < STREAM_RETRY_MS) {
      vTaskDelay(pdMS_TO_TICKS(20));
      continue;
    }

    lastStreamAttemptMs = millis();

    bool connected = false;
    StreamMode tried = settings.preferredMode;

    if (tried == STREAM_MODE_TCP) {
      connected = connectRawTcp();
    } else {
      connected = connectHttpWav();
    }

    if (!connected && settings.autoFallback && !stopRequested) {
      streamClient.stop();
      endI2S();
      ringClear();

      StreamMode fallback = tried == STREAM_MODE_TCP ? STREAM_MODE_HTTP : STREAM_MODE_TCP;
      Serial.printf("[STREAM] Primary failed; trying %s
", streamModeName(fallback));

      if (fallback == STREAM_MODE_TCP) {
        connected = connectRawTcp();
      } else {
        connected = connectHttpWav();
      }
    }

    if (connected) {
      stats.reconnects++;
      runConnectedStream();
    } else {
      stats.streamErrors++;
      setReceiverState(RX_ERROR, stats.lastError.length() ? stats.lastError : "Stream unavailable");
    }

    streamClient.stop();
    endI2S();
    ringClear();

    if (!stopRequested && settings.streamEnabled && settings.autoReconnect) {
      vTaskDelay(pdMS_TO_TICKS(STREAM_RETRY_MS));
    } else {
      vTaskDelay(pdMS_TO_TICKS(100));
    }
  }
}

// ============================================================================
// 13. STREAM CONTROLS
// ============================================================================

static void startStreaming() {
  stopRequested = false;
  settings.streamEnabled = true;
  saveSettings();
  setReceiverState(RX_IDLE);
}

static void stopStreaming() {
  stopRequested = true;
  settings.streamEnabled = false;
  saveSettings();

  streamClient.stop();
  ringClear();
  endI2S();
  setReceiverState(RX_STOPPED);
}

static void reconnectStreaming() {
  stopRequested = true;
  streamClient.stop();
  ringClear();
  endI2S();

  delay(100);

  stopRequested = false;
  settings.streamEnabled = true;
  saveSettings();
  setReceiverState(RX_IDLE);
}

// ============================================================================
// 14. WEB API HELPERS
// ============================================================================

static String jsonEscape(const String &input) {
  String output;
  output.reserve(input.length() + 8);

  for (size_t i = 0; i < input.length(); ++i) {
    char c = input[i];

    switch (c) {
      case '\\': output += "\\\\"; break;
      case '"': output += "\\""; break;
      case '
': output += "\
"; break;
      case '
': output += "\\r"; break;
      case '\t': output += "\\t"; break;
      default:
        if ((uint8_t)c >= 0x20) {
          output += c;
        }
        break;
    }
  }

  return output;
}

static String makeStatusJson() {
  String ip = WiFi.status() == WL_CONNECTED ? WiFi.localIP().toString() : "";
  String ssid = WiFi.status() == WL_CONNECTED ? WiFi.SSID() : "";
  int rssi = WiFi.status() == WL_CONNECTED ? WiFi.RSSI() : 0;

  uint32_t sessionSeconds = 0;
  if (stats.sessionStartedMs > 0 &&
      (receiverState == RX_STREAMING || receiverState == RX_BUFFERING)) {
    sessionSeconds = (millis() - stats.sessionStartedMs) / 1000UL;
  }

  String result;
  result.reserve(1100);

  result += "{";
  result += ""device":"" + jsonEscape(DEVICE_NAME) + "",";
  result += ""version":"" + String(FIRMWARE_VERSION) + "",";
  result += ""state":"" + String(receiverStateName(receiverState)) + "",";
  result += ""stateCode":" + String((int)receiverState) + ",";
  result += ""mode":"" + String(streamModeName(settings.preferredMode)) + "",";
  result += ""streamEnabled":" + String(settings.streamEnabled ? "true" : "false") + ",";
  result += ""wifiConnected":" + String(WiFi.status() == WL_CONNECTED ? "true" : "false") + ",";
  result += ""ssid":"" + jsonEscape(ssid) + "",";
  result += ""ip":"" + jsonEscape(ip) + "",";
  result += ""hostname":"" + String(MDNS_HOSTNAME) + ".local",";
  result += ""rssi":" + String(rssi) + ",";
  result += ""host":"" + jsonEscape(settings.phoneHost) + "",";
  result += ""tcpPort":" + String(settings.tcpPort) + ",";
  result += ""httpPort":" + String(settings.httpPort) + ",";
  result += ""sampleRate":" + String(streamFormat.sampleRate) + ",";
  result += ""channels":" + String(streamFormat.channels) + ",";
  result += ""bits":" + String(streamFormat.bitsPerSample) + ",";
  result += ""bufferBytes":" + String((uint32_t)ringSize()) + ",";
  result += ""bufferPercent":" + String(ringPercent()) + ",";
  result += ""bytesReceived":" + String(stats.bytesReceived) + ",";
  result += ""bytesPlayed":" + String(stats.bytesPlayed) + ",";
  result += ""underruns":" + String(stats.underruns) + ",";
  result += ""reconnects":" + String(stats.reconnects) + ",";
  result += ""streamErrors":" + String(stats.streamErrors) + ",";
  result += ""httpStatus":" + String(stats.lastHttpStatus) + ",";
  result += ""sessionSeconds":" + String(sessionSeconds) + ",";
  result += ""heap":" + String(ESP.getFreeHeap()) + ",";
  result += ""flashSize":" + String(ESP.getFlashChipSize()) + ",";
  result += ""oled":" + String(settings.oledEnabled ? "true" : "false") + ",";
  result += ""lastError":"" + jsonEscape(stats.lastError) + """;
  result += "}";

  return result;
}

static String makeConfigJson() {
  String result;
  result.reserve(500);

  result += "{";
  result += ""host":"" + jsonEscape(settings.phoneHost) + "",";
  result += ""tcpPort":" + String(settings.tcpPort) + ",";
  result += ""httpPort":" + String(settings.httpPort) + ",";
  result += ""mode":"" + String(settings.preferredMode == STREAM_MODE_HTTP ? "http" : "tcp") + "",";
  result += ""autoFallback":" + String(settings.autoFallback ? "true" : "false") + ",";
  result += ""autoReconnect":" + String(settings.autoReconnect ? "true" : "false") + ",";
  result += ""oled":" + String(settings.oledEnabled ? "true" : "false") + ",";
  result += ""streamEnabled":" + String(settings.streamEnabled ? "true" : "false") + ",";
  result += ""bufferMs":" + String(settings.targetBufferMs) + ",";
  result += ""i2s":{"bclk":" + String(I2S_BCLK_PIN) +
            ","lrclk":" + String(I2S_LRCLK_PIN) +
            ","dout":" + String(I2S_DOUT_PIN) + "}";
  result += "}";

  return result;
}

static void sendJson(int statusCode, const String &body) {
  server.sendHeader("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0");
  server.sendHeader("Pragma", "no-cache");
  server.send(statusCode, "application/json", body);
}

static bool requirePost() {
  if (server.method() != HTTP_POST) {
    sendJson(405, "{"ok":false,"error":"POST required"}");
    return false;
  }

  return true;
}

// ============================================================================
// 15. EMBEDDED MATERIAL-INSPIRED WEB APP
// ============================================================================

static String htmlPage() {
  return R"HTML(<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#14121f">
<title>C3 Music Receiver</title>
<style>
:root{--bg:#14121f;--surface:#1e1b2e;--surface2:#28233a;--primary:#cbb8ff;--onprimary:#2f1765;--text:#e7e0f4;--muted:#c9c0d5;--outline:#938f9d;--ok:#87e8ae;--warn:#ffd180;--bad:#ffb4ab;--shadow:0 10px 32px rgba(0,0,0,.25)}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;font-size:14px}
header{position:sticky;top:0;z-index:10;background:rgba(20,18,31,.94);backdrop-filter:blur(12px);border-bottom:1px solid rgba(255,255,255,.08);padding:15px 16px 10px}
.headrow{display:flex;align-items:center;justify-content:space-between;gap:12px;max-width:980px;margin:auto}.title{font-size:21px;font-weight:700;letter-spacing:.1px}.sub{color:var(--muted);font-size:12px;margin-top:2px}.chip{border:1px solid var(--outline);border-radius:999px;padding:6px 10px;font-size:12px;white-space:nowrap}.chip.ok{border-color:#4b9965;color:var(--ok)}.chip.warn{border-color:#a27936;color:var(--warn)}.chip.bad{border-color:#a65351;color:var(--bad)}
main{max-width:980px;margin:0 auto;padding:16px 16px 96px}.tab{display:none}.tab.active{display:block}.grid{display:grid;grid-template-columns:repeat(12,1fr);gap:14px}.card{grid-column:span 12;background:var(--surface);border:1px solid rgba(255,255,255,.09);border-radius:22px;padding:17px;box-shadow:var(--shadow)}@media(min-width:720px){.half{grid-column:span 6}.third{grid-column:span 4}}
h2{font-size:18px;margin:0 0 14px}h3{font-size:14px;margin:0 0 10px;color:var(--muted);font-weight:600}.hero{font-size:27px;font-weight:750;letter-spacing:.2px}.meta{margin-top:5px;color:var(--muted)}
.row{display:flex;justify-content:space-between;align-items:center;gap:14px;padding:9px 0;border-bottom:1px solid rgba(255,255,255,.07)}.row:last-child{border-bottom:0}.label{color:var(--muted)}.value{text-align:right;overflow-wrap:anywhere}.meter{height:10px;border-radius:99px;background:#403a50;overflow:hidden;margin:12px 0 5px}.meter>i{display:block;height:100%;border-radius:99px;background:linear-gradient(90deg,#bb86fc,#d7c4ff);width:0;transition:width .25s}
button{border:0;border-radius:999px;background:var(--primary);color:var(--onprimary);font-weight:750;padding:11px 16px;cursor:pointer;font-size:14px}button.secondary{background:transparent;color:var(--primary);border:1px solid #8c78b7}button.danger{background:#ffb4ab;color:#690005}button:disabled{opacity:.55;cursor:not-allowed}.buttons{display:flex;flex-wrap:wrap;gap:9px;margin-top:14px}
.field{margin:13px 0}.field label{display:block;color:var(--muted);font-size:12px;margin:0 0 6px 4px}input,select{width:100%;border:1px solid var(--outline);background:#272334;color:var(--text);border-radius:12px;padding:12px;font:inherit;outline:none}input:focus,select:focus{border-color:var(--primary);box-shadow:0 0 0 3px rgba(203,184,255,.15)}
.switchrow{display:flex;align-items:center;justify-content:space-between;gap:14px;padding:11px 0}.switch{appearance:none;width:46px;height:26px;margin:0;border-radius:20px;background:#5c5666;position:relative;cursor:pointer;border:0;flex:none}.switch:checked{background:#cbb8ff}.switch:before{content:"";position:absolute;width:20px;height:20px;top:3px;left:3px;background:#fff;border-radius:50%;transition:.18s}.switch:checked:before{left:23px;background:#34205f}
nav{position:fixed;z-index:20;bottom:0;left:0;right:0;display:flex;justify-content:center;gap:5px;padding:9px max(10px,env(safe-area-inset-right)) calc(9px + env(safe-area-inset-bottom)) max(10px,env(safe-area-inset-left));background:rgba(30,27,46,.96);backdrop-filter:blur(12px);border-top:1px solid rgba(255,255,255,.09)}nav button{min-width:100px;background:transparent;color:var(--muted);padding:9px 13px}nav button.active{background:#4c3e6d;color:#f0e7ff}.small{font-size:12px;color:var(--muted)}.mono{font-family:ui-monospace,SFMono-Regular,Consolas,monospace;font-size:12px;word-break:break-word}.hidden{display:none!important}
#toast{position:fixed;z-index:50;left:50%;bottom:82px;transform:translate(-50%,25px);opacity:0;pointer-events:none;background:#ece6f5;color:#201b29;border-radius:12px;padding:12px 15px;box-shadow:var(--shadow);transition:.2s;max-width:calc(100vw - 32px)}#toast.show{opacity:1;transform:translate(-50%,0)}progress{width:100%;height:11px;accent-color:#cbb8ff}
</style>
</head>
<body>
<header><div class="headrow"><div><div class="title">C3 Music Receiver</div><div class="sub" id="address">c3music.local</div></div><div class="chip" id="stateChip">Loading</div></div></header>
<main>
<section class="tab active" id="now">
<div class="grid">
<div class="card"><h2>Now Playing</h2><div class="hero" id="state">Connecting…</div><div class="meta" id="format">Waiting for receiver status</div><div class="meter"><i id="bufferBar"></i></div><div class="small" id="bufferText">Buffer: —</div><div class="buttons"><button onclick="action('/api/stream/start')">Start</button><button class="secondary" onclick="action('/api/stream/stop')">Stop</button><button class="secondary" onclick="action('/api/stream/reconnect')">Reconnect</button></div></div>
<div class="card half"><h3>Source</h3><div class="row"><span class="label">Preferred mode</span><span class="value" id="mode">—</span></div><div class="row"><span class="label">Phone host</span><span class="value" id="host">—</span></div><div class="row"><span class="label">Session</span><span class="value" id="session">—</span></div></div>
<div class="card half"><h3>Audio health</h3><div class="row"><span class="label">Underruns</span><span class="value" id="underruns">0</span></div><div class="row"><span class="label">Reconnects</span><span class="value" id="reconnects">0</span></div><div class="row"><span class="label">Last error</span><span class="value" id="lastError">None</span></div></div>
</div>
</section>
<section class="tab" id="wifi">
<div class="grid">
<div class="card"><h2>Wi‑Fi</h2><div class="row"><span class="label">Status</span><span class="value" id="wifiStatus">—</span></div><div class="row"><span class="label">SSID</span><span class="value" id="ssid">—</span></div><div class="row"><span class="label">IP address</span><span class="value" id="ip">—</span></div><div class="row"><span class="label">Hostname</span><span class="value">c3music.local</span></div><div class="row"><span class="label">Signal</span><span class="value" id="rssi">—</span></div><div class="buttons"><button onclick="action('/api/wifi/reconnect')">Reconnect Wi‑Fi</button></div></div>
<div class="card"><h3>About this build</h3><p class="small">Wi‑Fi credentials are intentionally embedded in <span class="mono">main.cpp</span>. Change them in source and use browser OTA after the first flash if you move to another network.</p></div>
</div>
</section>
<section class="tab" id="settings">
<div class="grid">
<div class="card half"><h2>Stream settings</h2>
<div class="field"><label for="hostInput">Phone host / IP</label><input id="hostInput" autocomplete="off"></div>
<div class="field"><label for="modeInput">Preferred stream</label><select id="modeInput"><option value="tcp">Raw TCP PCM — port 50005</option><option value="http">HTTP WAV/PCM — port 8080</option></select></div>
<div class="field"><label for="tcpInput">TCP port</label><input id="tcpInput" type="number" min="1" max="65535"></div>
<div class="field"><label for="httpInput">HTTP port</label><input id="httpInput" type="number" min="1" max="65535"></div>
<div class="switchrow"><span>Automatic TCP/HTTP fallback</span><input class="switch" id="fallbackInput" type="checkbox"></div>
<div class="switchrow"><span>Automatic stream reconnect</span><input class="switch" id="autoreconnectInput" type="checkbox"></div>
<div class="buttons"><button onclick="saveConfig()">Save and reconnect</button></div>
</div>
<div class="card half"><h2>Device settings</h2>
<div class="switchrow"><span>OLED display enabled</span><input class="switch" id="oledInput" type="checkbox" onchange="saveConfig()"></div>
<div class="row"><span class="label">I2S BCLK</span><span class="value">GPIO 4</span></div><div class="row"><span class="label">I2S LRCLK</span><span class="value">GPIO 7</span></div><div class="row"><span class="label">I2S data</span><span class="value">GPIO 10</span></div>
<div class="row"><span class="label">Firmware</span><span class="value" id="version">—</span></div><div class="row"><span class="label">Free heap</span><span class="value" id="heap">—</span></div>
</div>
<div class="card"><h2>Firmware OTA update</h2><p class="small">Select a PlatformIO firmware <span class="mono">.bin</span> file. Streaming will stop while the firmware is written, then the device will restart only after a successful update.</p><input id="firmware" type="file" accept=".bin,application/octet-stream"><div class="buttons"><button id="otaButton" onclick="uploadFirmware()">Install firmware</button></div><progress id="otaProgress" class="hidden" value="0" max="100"></progress><div class="small" id="otaText"></div></div>
<div class="card"><h2>Maintenance</h2><div class="buttons"><button class="secondary" onclick="action('/api/system/clear-stats')">Clear statistics</button><button class="secondary" onclick="action('/api/system/reboot')">Restart device</button><button class="danger" onclick="factoryReset()">Factory reset</button></div></div>
</div>
</section>
</main>
<nav><button class="active" data-tab="now">Now Playing</button><button data-tab="wifi">Wi‑Fi</button><button data-tab="settings">Settings</button></nav><div id="toast"></div>
<script>
const $=id=>document.getElementById(id);let statusData={},configLoaded=false;
function toast(t){const x=$('toast');x.textContent=t;x.classList.add('show');setTimeout(()=>x.classList.remove('show'),2800)}
function text(id,v){const e=$(id);if(e)e.textContent=v}
function time(s){s=Number(s||0);const h=Math.floor(s/3600),m=Math.floor(s%3600/60),q=s%60;return[h,m,q].map((x,i)=>i===0?String(x).padStart(2,'0'):String(x).padStart(2,'0')).join(':')}
function applyStatus(d){statusData=d;text('state',d.state||'—');text('format',`${d.sampleRate||0} Hz · ${d.bits||0}-bit · ${d.channels===2?'Stereo':'Mono'} · PCM pass-through`);text('mode',d.mode||'—');text('host',`${d.host||'—'} · ${d.mode==='HTTP WAV'?d.httpPort:d.tcpPort}`);text('session',time(d.sessionSeconds));text('underruns',d.underruns||0);text('reconnects',d.reconnects||0);text('lastError',d.lastError||'None');text('wifiStatus',d.wifiConnected?'Connected':'Disconnected');text('ssid',d.ssid||'—');text('ip',d.ip||'—');text('rssi',d.wifiConnected?`${d.rssi} dBm`:'—');text('address',d.ip?`http://${d.hostname||'c3music.local'} · ${d.ip}`:'c3music.local');text('version',d.version||'—');text('heap',d.heap?`${Math.round(d.heap/1024)} KB`:'—');const p=Number(d.bufferPercent||0);$('bufferBar').style.width=p+'%';text('bufferText',`Buffer: ${p}% · ${d.bufferBytes||0} bytes`);const c=$('stateChip');c.textContent=d.state||'Unknown';c.className='chip '+(d.state==='Streaming'?'ok':d.state==='Error'||d.state==='WiFi offline'?'bad':'warn')}
async function poll(){try{const r=await fetch('/api/status',{cache:'no-store'});if(!r.ok)throw 0;applyStatus(await r.json());if(!configLoaded)loadConfig()}catch(e){text('state','Web connection lost');$('stateChip').textContent='Offline';$('stateChip').className='chip bad'}}
async function loadConfig(){try{const d=await (await fetch('/api/config',{cache:'no-store'})).json();$('hostInput').value=d.host||'';$('tcpInput').value=d.tcpPort||50005;$('httpInput').value=d.httpPort||8080;$('modeInput').value=d.mode||'tcp';$('fallbackInput').checked=!!d.autoFallback;$('autoreconnectInput').checked=!!d.autoReconnect;$('oledInput').checked=!!d.oled;configLoaded=true}catch(e){}}
async function action(url){try{const r=await fetch(url,{method:'POST'});const d=await r.json();toast(d.ok?'Done':(d.error||'Failed'));setTimeout(poll,350)}catch(e){toast('Request failed')}}
async function saveConfig(){const body=new URLSearchParams({host:$('hostInput').value.trim(),tcpPort:$('tcpInput').value,httpPort:$('httpInput').value,mode:$('modeInput').value,autoFallback:$('fallbackInput').checked?'1':'0',autoReconnect:$('autoreconnectInput').checked?'1':'0',oled:$('oledInput').checked?'1':'0'});try{const r=await fetch('/api/config',{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded'},body});const d=await r.json();toast(d.ok?'Settings saved':'Save failed');configLoaded=false;setTimeout(poll,600)}catch(e){toast('Save failed')}}
function factoryReset(){if(confirm('Factory reset stream settings and stored options? The device will restart.'))action('/api/system/factory-reset')}
function uploadFirmware(){const f=$('firmware').files[0];if(!f){toast('Choose a .bin firmware file first');return}if(!confirm(`Install ${f.name}? Streaming will stop and the device will reboot.`))return;const xhr=new XMLHttpRequest(),form=new FormData();form.append('firmware',f);$('otaProgress').classList.remove('hidden');$('otaProgress').value=0;$('otaText').textContent='Uploading…';$('otaButton').disabled=true;xhr.upload.onprogress=e=>{if(e.lengthComputable){const p=Math.round(e.loaded/e.total*100);$('otaProgress').value=p;$('otaText').textContent=`Uploading ${p}%`}};xhr.onload=()=>{$('otaButton').disabled=false;if(xhr.status===200){$('otaProgress').value=100;$('otaText').textContent='Update accepted. Device is restarting…';toast('Firmware update successful')}else{$('otaText').textContent='Update failed: '+xhr.responseText;toast('OTA failed')}};xhr.onerror=()=>{$('otaButton').disabled=false;$('otaText').textContent='Upload connection failed';toast('OTA failed')};xhr.open('POST','/api/ota');xhr.send(form)}
document.querySelectorAll('nav button').forEach(b=>b.onclick=()=>{document.querySelectorAll('nav button').forEach(x=>x.classList.remove('active'));document.querySelectorAll('.tab').forEach(x=>x.classList.remove('active'));b.classList.add('active');$(b.dataset.tab).classList.add('active')});poll();setInterval(poll,1000);
</script></body></html>)HTML";
}

// ============================================================================
// 16. WEB ROUTES / OTA
// ============================================================================

static bool otaUploadFailed = false;
static String otaUploadError;

static void handleFirmwareUpload() {
  HTTPUpload &upload = server.upload();

  if (upload.status == UPLOAD_FILE_START) {
    otaUploadFailed = false;
    otaUploadError = "";

    stopRequested = true;
    streamClient.stop();
    ringClear();
    endI2S();
    setReceiverState(RX_UPDATING);

    size_t updateSize = UPDATE_SIZE_UNKNOWN;
    if (!Update.begin(updateSize, U_FLASH)) {
      otaUploadFailed = true;
      otaUploadError = Update.errorString();
      Serial.printf("[OTA] Begin failed: %s
", otaUploadError.c_str());
    } else {
      Serial.printf("[OTA] Start: %s
", upload.filename.c_str());
    }
  } else if (upload.status == UPLOAD_FILE_WRITE) {
    if (!otaUploadFailed) {
      size_t written = Update.write(upload.buf, upload.currentSize);

      if (written != upload.currentSize) {
        otaUploadFailed = true;
        otaUploadError = Update.errorString();
        Serial.printf("[OTA] Write failed: %s
", otaUploadError.c_str());
      }
    }
  } else if (upload.status == UPLOAD_FILE_END) {
    if (!otaUploadFailed) {
      if (!Update.end(true)) {
        otaUploadFailed = true;
        otaUploadError = Update.errorString();
        Serial.printf("[OTA] End failed: %s
", otaUploadError.c_str());
      } else {
        Serial.printf("[OTA] Success: %u bytes
", upload.totalSize);
      }
    }
  } else if (upload.status == UPLOAD_FILE_ABORTED) {
    otaUploadFailed = true;
    otaUploadError = "Upload aborted";
    Update.abort();
  }
}

static void setupWebServer() {
  server.on("/", HTTP_GET, []() {
    server.sendHeader("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0");
    server.send(200, "text/html; charset=utf-8", htmlPage());
  });

  server.on("/api/status", HTTP_GET, []() {
    sendJson(200, makeStatusJson());
  });

  server.on("/api/config", HTTP_GET, []() {
    sendJson(200, makeConfigJson());
  });

  server.on("/api/config", HTTP_POST, []() {
    if (!requirePost()) {
      return;
    }

    String host = server.arg("host");
    host.trim();

    if (host.length() == 0 || host.length() > 63) {
      sendJson(400, "{"ok":false,"error":"Invalid phone host"}");
      return;
    }

    uint32_t tcpPort = server.arg("tcpPort").toInt();
    uint32_t httpPort = server.arg("httpPort").toInt();

    if (tcpPort < 1 || tcpPort > 65535 || httpPort < 1 || httpPort > 65535) {
      sendJson(400, "{"ok":false,"error":"Invalid port"}");
      return;
    }

    settings.phoneHost = host;
    settings.tcpPort = (uint16_t)tcpPort;
    settings.httpPort = (uint16_t)httpPort;
    settings.preferredMode = server.arg("mode") == "http" ? STREAM_MODE_HTTP : STREAM_MODE_TCP;
    settings.autoFallback = server.arg("autoFallback") == "1";
    settings.autoReconnect = server.arg("autoReconnect") == "1";
    settings.oledEnabled = server.arg("oled") == "1";

    if (oledAvailable) {
      oled.setPowerSave(settings.oledEnabled ? 0 : 1);
    }

    saveSettings();
    reconnectStreaming();

    sendJson(200, "{"ok":true}");
  });

  server.on("/api/stream/start", HTTP_POST, []() {
    if (!requirePost()) {
      return;
    }

    startStreaming();
    sendJson(200, "{"ok":true}");
  });

  server.on("/api/stream/stop", HTTP_POST, []() {
    if (!requirePost()) {
      return;
    }

    stopStreaming();
    sendJson(200, "{"ok":true}");
  });

  server.on("/api/stream/reconnect", HTTP_POST, []() {
    if (!requirePost()) {
      return;
    }

    reconnectStreaming();
    sendJson(200, "{"ok":true}");
  });

  server.on("/api/wifi/reconnect", HTTP_POST, []() {
    if (!requirePost()) {
      return;
    }

    WiFi.disconnect(false, false);
    mdnsStarted = false;
    lastWifiAttemptMs = 0;
    connectWifiIfNeeded();

    sendJson(200, "{"ok":true}");
  });

  server.on("/api/system/clear-stats", HTTP_POST, []() {
    if (!requirePost()) {
      return;
    }

    stats = RuntimeStats();
    sendJson(200, "{"ok":true}");
  });

  server.on("/api/system/reboot", HTTP_POST, []() {
    if (!requirePost()) {
      return;
    }

    sendJson(200, "{"ok":true,"message":"Restarting"}");
    delay(250);
    ESP.restart();
  });

  server.on("/api/system/factory-reset", HTTP_POST, []() {
    if (!requirePost()) {
      return;
    }

    resetSettings();
    sendJson(200, "{"ok":true,"message":"Settings reset; restarting"}");
    delay(250);
    ESP.restart();
  });

  server.on(
    "/api/ota",
    HTTP_POST,
    []() {
      if (otaUploadFailed) {
        String body = "{"ok":false,"error":"" + jsonEscape(otaUploadError) + ""}";
        sendJson(500, body);

        stopRequested = false;
        settings.streamEnabled = true;
        setReceiverState(RX_IDLE);
      } else {
        sendJson(200, "{"ok":true,"message":"Firmware written; restarting"}");
        delay(700);
        ESP.restart();
      }
    },
    handleFirmwareUpload
  );

  server.onNotFound([]() {
    sendJson(404, "{"ok":false,"error":"Not found"}");
  });

  server.begin();
  Serial.println("[WEB] HTTP server started on port 80");
}

// ============================================================================
// 17. ARDUINO SETUP / LOOP
// ============================================================================

void setup() {
  Serial.begin(115200);
  delay(400);

  Serial.println();
  Serial.println("============================================================");
  Serial.printf("%s v%s
", DEVICE_NAME, FIRMWARE_VERSION);
  Serial.println("Raw PCM Wi-Fi receiver — TCP primary / HTTP WAV fallback");
  Serial.println("============================================================");

  loadSettings();

  audioDataSemaphore = xSemaphoreCreateBinary();

  displayBegin();
  setReceiverState(RX_BOOTING);

  WiFi.persistent(false);
  WiFi.mode(WIFI_STA);
  WiFi.setSleep(false);
  WiFi.setAutoReconnect(true);

  setupWebServer();

  BaseType_t playbackCreated = xTaskCreate(
    playbackTask,
    "i2sPlayback",
    4096,
    nullptr,
    3,
    &playbackTaskHandle
  );

  BaseType_t streamCreated = xTaskCreate(
    streamTask,
    "pcmStream",
    6144,
    nullptr,
    2,
    &streamTaskHandle
  );

  if (playbackCreated != pdPASS || streamCreated != pdPASS) {
    setReceiverState(RX_ERROR, "Task creation failed");
  }

  connectWifiIfNeeded();
  displayUpdate();
}

void loop() {
  server.handleClient();

  if (WiFi.status() != WL_CONNECTED) {
    mdnsStarted = false;

    if (receiverState != RX_WIFI_CONNECTING &&
        receiverState != RX_UPDATING &&
        settings.streamEnabled) {
      setReceiverState(RX_WIFI_OFFLINE);
    }

    connectWifiIfNeeded();
  } else {
    beginMdnsIfNeeded();
  }

  displayUpdate();

  if (millis() - lastStatusRefreshMs >= STATUS_REFRESH_MS) {
    lastStatusRefreshMs = millis();

    if (receiverState == RX_STREAMING &&
        ringSize() < (PREBUFFER_BYTES / 8)) {
      setReceiverState(RX_BUFFERING, "Buffer below target");
    } else if (receiverState == RX_BUFFERING &&
               bufferStarted &&
               ringSize() >= (PREBUFFER_BYTES / 2)) {
      setReceiverState(RX_STREAMING);
    }
  }

  delay(2);
}
