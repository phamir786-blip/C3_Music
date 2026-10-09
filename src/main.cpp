/*
  C3 Music Receiver (UDP Edition)
  ESP32-C3 + SSD1306 72x40 OLED + UDA1334A I2S DAC
  High-Performance Low-Latency UDP Audio Receiver (Port 50005)
  OLED: SDA 5, SCL 6
  I2S: BCLK 3, LRCLK 1, DOUT 10
*/

#include <Arduino.h>
#include <WiFi.h>
#include <WiFiUdp.h>
#include <WebServer.h>
#include <ESPmDNS.h>
#include <Preferences.h>
#include <Update.h>
#include <U8g2lib.h>
#include <Wire.h>
#include <driver/i2s.h>
#include <math.h>

static const char WIFI_SSID[] = "GFiber_2.4_Coverage_AECD9";
static const char WIFI_PASSWORD[] = "006BF4FD";
static const char DEVICE_NAME[] = "C3 Music Receiver";
static const char MDNS_HOSTNAME[] = "c3music";
static const char FIRMWARE_VERSION[] = "2.0.0";

static constexpr uint8_t OLED_SDA = 5;
static constexpr uint8_t OLED_SCL = 6;
static constexpr uint8_t OLED_WIDTH = 72;
static constexpr uint8_t OLED_HEIGHT = 40;

static constexpr int I2S_BCLK_PIN = 3;
static constexpr int I2S_LRCLK_PIN = 1;
static constexpr int I2S_DOUT_PIN = 10;

static constexpr uint16_t UDP_DEFAULT_PORT = 50005;

static constexpr uint32_t DEFAULT_SAMPLE_RATE = 44100;
static constexpr uint8_t DEFAULT_CHANNELS = 2;
static constexpr uint8_t DEFAULT_BITS_PER_SAMPLE = 16;

static constexpr uint8_t C3_PROTOCOL_VERSION = 1;
static constexpr size_t C3_FORMAT_HEADER_BYTES = 16;

// 64 KB Ring Buffer for stable streaming
static constexpr size_t AUDIO_RING_BYTES = 65536;
static constexpr size_t UDP_MAX_PACKET_BYTES = 1472;
static constexpr size_t I2S_WRITE_BYTES = 2048;
static constexpr uint32_t PREBUFFER_BYTES = 12000;
static constexpr uint32_t WIFI_RETRY_MS = 10000;
static constexpr uint32_t OLED_REFRESH_MS = 650;
static constexpr uint32_t STATUS_REFRESH_MS = 1000;
static constexpr uint32_t STREAM_READ_TIMEOUT_MS = 5000;

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
  uint16_t udpPort = UDP_DEFAULT_PORT;
  bool autoReconnect = true, oledEnabled = true, streamEnabled = true;
  uint16_t targetBufferMs = 250;
  uint8_t volumePercent = 50;
};

static U8G2_SSD1306_72X40_ER_F_HW_I2C oled(U8G2_R0, U8X8_PIN_NONE);
static bool oledAvailable = false;
static bool oledStreamingDisabled = false;
static Preferences preferences;
static WebServer server(80);
static WiFiUDP udpClient;
static Settings settings;
static StreamFormat streamFormat;
static RuntimeStats stats;
static volatile ReceiverState receiverState = RX_BOOTING;
static volatile bool stopRequested = false;
static volatile bool i2sReady = false;
static volatile bool bufferStarted = false;
static volatile bool udpListening = false;
static uint8_t audioRing[AUDIO_RING_BYTES];
static volatile size_t ringReadIndex = 0, ringWriteIndex = 0, ringCount = 0;
static portMUX_TYPE ringMux = portMUX_INITIALIZER_UNLOCKED;
static SemaphoreHandle_t audioDataSemaphore = nullptr;
static SemaphoreHandle_t i2sMux = nullptr;
static SemaphoreHandle_t udpMux = nullptr;
static TaskHandle_t streamTaskHandle = nullptr, playbackTaskHandle = nullptr;
static uint32_t lastWifiAttemptMs = 0, lastOledRefreshMs = 0, lastStatusRefreshMs = 0;
static bool mdnsStarted = false;

static void loadSettings() {
  preferences.begin("c3music", false);
  settings.udpPort = preferences.getUShort("udpport", UDP_DEFAULT_PORT);
  settings.autoReconnect = preferences.getBool("autorecon", true);
  settings.oledEnabled = preferences.getBool("oled", true);
  settings.streamEnabled = preferences.getBool("enabled", true);
  settings.targetBufferMs = preferences.getUShort("buffer", 250);
  settings.volumePercent = preferences.getUChar("volume", 50);
  if (settings.volumePercent > 100) settings.volumePercent = 100;
  if (settings.udpPort == 0) settings.udpPort = UDP_DEFAULT_PORT;
  if (settings.targetBufferMs < 80) settings.targetBufferMs = 80;
  if (settings.targetBufferMs > 700) settings.targetBufferMs = 700;
}

static void saveSettings() {
  preferences.putUShort("udpport", settings.udpPort);
  preferences.putBool("autorecon", settings.autoReconnect);
  preferences.putBool("oled", settings.oledEnabled);
  preferences.putBool("enabled", settings.streamEnabled);
  preferences.putUShort("buffer", settings.targetBufferMs);
  preferences.putUChar("volume", settings.volumePercent);
}

static void resetSettings() {
  preferences.clear(); preferences.end();
  settings.udpPort = UDP_DEFAULT_PORT;
  settings.autoReconnect = true;
  settings.oledEnabled = true; settings.streamEnabled = true;
  settings.targetBufferMs = 250;
  settings.volumePercent = 50;
  preferences.begin("c3music", false); saveSettings();
}

static const char* receiverStateName(ReceiverState v) {
  switch(v) {
    case RX_BOOTING: return "Booting"; case RX_WIFI_CONNECTING: return "WiFi connecting";
    case RX_WIFI_OFFLINE: return "WiFi offline"; case RX_IDLE: return "Ready";
    case RX_CONNECTING: return "Connecting"; case RX_BUFFERING: return "Buffering";
    case RX_STREAMING: return "Streaming"; case RX_STOPPED: return "Stopped";
    case RX_ERROR: return "Error"; case RX_UPDATING: return "Updating";
    default: return "Unknown";
  }
}

static void setReceiverState(ReceiverState value, const String &error = "") {
  receiverState = value;
  if (error.length() > 0) { stats.lastError = error; Serial.printf("[STATE] %s: %s\n", receiverStateName(value), error.c_str()); }
  else { Serial.printf("[STATE] %s\n", receiverStateName(value)); }
}

// Compiler type-safe ringWrite
static bool ringWrite(const uint8_t *data, size_t length) {
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

// Compiler type-safe ringRead
static size_t ringRead(uint8_t *out, size_t maxLen) {
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

static void ringClear() {
  portENTER_CRITICAL(&ringMux);
  ringReadIndex = ringWriteIndex = ringCount = 0;
  portEXIT_CRITICAL(&ringMux);
  bufferStarted = false;
}

static size_t ringSize() { portENTER_CRITICAL(&ringMux); size_t v = (size_t)ringCount; portEXIT_CRITICAL(&ringMux); return v; }
static uint8_t ringPercent() { return (uint8_t)((ringSize() * 100UL) / AUDIO_RING_BYTES); }

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
  int w = oled.getStrWidth(text);
  int x = (OLED_WIDTH - w) / 2; if (x < 0) x = 0;
  oled.drawStr(x, y, text);
}

static void displayUpdate() {
  if (!oledAvailable || !settings.oledEnabled) return;

  if (receiverState == RX_STREAMING) {
    if (!oledStreamingDisabled) {
      oled.setPowerSave(1);
      oledStreamingDisabled = true;
      Serial.println("[OLED] Deactivated during streaming");
    }
    return;
  }

  if (oledStreamingDisabled) return;

  if (millis() - lastOledRefreshMs < OLED_REFRESH_MS) return;
  lastOledRefreshMs = millis();
  char buf[32];
  oled.clearBuffer();
  oled.setFont(u8g2_font_5x7_tf);
  oled.drawStr(0, 7, "C3 MUSIC");
  const char *st = "BOOT";
  switch(receiverState) {
    case RX_WIFI_CONNECTING: st = "WIFI"; break;
    case RX_WIFI_OFFLINE: st = "NO WIFI"; break;
    case RX_IDLE: st = "READY"; break;
    case RX_CONNECTING: st = "LINK"; break;
    case RX_BUFFERING: st = "BUFFER"; break;
    case RX_STREAMING: st = "PLAY"; break;
    case RX_STOPPED: st = "STOP"; break;
    case RX_ERROR: st = "ERROR"; break;
    case RX_UPDATING: st = "OTA"; break;
  }
  int sw = oled.getStrWidth(st);
  oled.drawStr(OLED_WIDTH - sw, 7, st);
  if (receiverState == RX_STREAMING || receiverState == RX_BUFFERING) {
    snprintf(buf, sizeof(buf), "%luk %s", (unsigned long)(streamFormat.sampleRate/1000), streamFormat.channels==2?"ST":"MO");
    displayLineCenter(buf, 19, u8g2_font_6x10_tf);
    snprintf(buf, sizeof(buf), "BUF %u%%", ringPercent());
    displayLineCenter(buf, 29, u8g2_font_4x6_tf);
    oled.drawFrame(0,33,OLED_WIDTH,7);    uint8_t w = (uint8_t)(((OLED_WIDTH-2)*ringPercent())/100);    if (w>0) oled.drawBox(1,34,w,5);
  } else {
    String msg;
    if (WiFi.status()==WL_CONNECTED) msg = WiFi.localIP().toString();
    else if (receiverState==RX_WIFI_CONNECTING) msg = "Connecting...";
    else msg = "Check WiFi";
    displayLineCenter(msg.c_str(), 22, u8g2_font_6x10_tf);
    if (receiverState==RX_ERROR && stats.lastError.length()>0) {
      String e = stats.lastError; if (e.length()>17) e=e.substring(0,17);
      displayLineCenter(e.c_str(), 38, u8g2_font_4x6_tf);
    } else {
      displayLineCenter("c3music.local", 38, u8g2_font_4x6_tf);
    }
  }
  oled.sendBuffer();
}

static void beginMdnsIfNeeded();

static void connectWifiIfNeeded() {
  if (WiFi.status()==WL_CONNECTED) { beginMdnsIfNeeded(); return; }
  if (millis()-lastWifiAttemptMs < WIFI_RETRY_MS) return;
  lastWifiAttemptMs = millis();
  setReceiverState(RX_WIFI_CONNECTING);
  WiFi.mode(WIFI_STA); WiFi.setHostname(MDNS_HOSTNAME);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
  Serial.printf("[WIFI] Connecting to %s\n", WIFI_SSID);
}

static void beginMdnsIfNeeded() {
  if (mdnsStarted || WiFi.status()!=WL_CONNECTED) return;
  if (MDNS.begin(MDNS_HOSTNAME)) { MDNS.addService("http","tcp",80); mdnsStarted=true; Serial.printf("[MDNS] http://%s.local/\n", MDNS_HOSTNAME); }
  else Serial.println("[MDNS] Failed");
}

static void endI2S() {
  if (i2sMux) xSemaphoreTake(i2sMux, portMAX_DELAY);
  if (i2sReady) { i2s_driver_uninstall(I2S_NUM_0); i2sReady=false; }
  if (i2sMux) xSemaphoreGive(i2sMux);
}

static uint32_t targetPrebufferBytes() {
  uint64_t bytesPerSecond = (uint64_t)streamFormat.sampleRate *
                            (uint64_t)streamFormat.channels *
                            (uint64_t)(streamFormat.bitsPerSample / 8);
  if (bytesPerSecond == 0) bytesPerSecond =
      (uint64_t)DEFAULT_SAMPLE_RATE * DEFAULT_CHANNELS * (DEFAULT_BITS_PER_SAMPLE / 8);
  uint64_t bytes = (bytesPerSecond * settings.targetBufferMs) / 1000ULL;
  if (bytes < 4096) bytes = 4096;
  const uint64_t maxBufferBytes = (uint64_t)AUDIO_RING_BYTES - 4096ULL;
  if (bytes > maxBufferBytes) bytes = maxBufferBytes;
  return (uint32_t)bytes;
}

static void stopUdpListener() {
  if (udpMux) xSemaphoreTake(udpMux, portMAX_DELAY);
  if (udpListening) {
    udpClient.stop();
    udpListening = false;
  }
  if (udpMux) xSemaphoreGive(udpMux);
}

static bool beginI2S(uint32_t sr, uint16_t ch, uint16_t bits) {
  if (bits!=16 || (ch!=1 && ch!=2)) return false;
  
  if (i2sMux) xSemaphoreTake(i2sMux, portMAX_DELAY);
  if (i2sReady) { i2s_driver_uninstall(I2S_NUM_0); i2sReady=false; }

  i2s_bits_per_sample_t bps = I2S_BITS_PER_SAMPLE_16BIT;

  i2s_config_t cfg = {
    .mode = (i2s_mode_t)(I2S_MODE_MASTER|I2S_MODE_TX),
    .sample_rate = sr,
    .bits_per_sample = bps,
    .channel_format = I2S_CHANNEL_FMT_RIGHT_LEFT,
    .communication_format = I2S_COMM_FORMAT_STAND_I2S,
    .intr_alloc_flags = ESP_INTR_FLAG_LEVEL1,
    .dma_buf_count = 4,
    .dma_buf_len = 1024,
    .use_apll = false,          // ESP32-C3 does NOT support APLL!
    .tx_desc_auto_clear = true,  // Automatically clears DMA buffer on underrun
    .fixed_mclk = 0             // Must be 0!
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
  i2sReady = true;
  if (i2sMux) xSemaphoreGive(i2sMux);

  Serial.printf("[I2S] %lu Hz, %u-bit, %s, BCLK=%d WS=%d DOUT=%d\n", (unsigned long)sr, bits, ch==2?"Stereo":"Mono", I2S_BCLK_PIN, I2S_LRCLK_PIN, I2S_DOUT_PIN);
  return true;
}

static uint32_t readLe32(const uint8_t *p){ return (uint32_t)p[0]|((uint32_t)p[1]<<8)|((uint32_t)p[2]<<16)|((uint32_t)p[3]<<24); }

// Parse optional 16-byte C3 header from UDP packet
static bool parseC3HeaderIfPresent(const uint8_t *pkt, size_t len, size_t &payloadOffset) {
  payloadOffset = 0;
  if (len < C3_FORMAT_HEADER_BYTES) return false;
  if (memcmp(pkt, "C3MS", 4) != 0) return false;

  const uint8_t version = pkt[4];
  const uint16_t bits = pkt[5];
  const uint16_t channels = pkt[6];
  const uint32_t sampleRate = readLe32(pkt + 8);
  const uint32_t frameSize = readLe32(pkt + 12);
  const uint32_t expectedFrameSize = (uint32_t)channels * (uint32_t)(bits / 8);

  if (version == C3_PROTOCOL_VERSION &&
      bits == 16 &&
      (channels == 1 || channels == 2) &&
      (sampleRate == 44100 || sampleRate == 48000) &&
      frameSize == expectedFrameSize &&
      expectedFrameSize > 0) {
    streamFormat.sampleRate = sampleRate;
    streamFormat.channels = channels;
    streamFormat.bitsPerSample = bits;
    streamFormat.audioFormat = 1;
    streamFormat.valid = true;
    payloadOffset = C3_FORMAT_HEADER_BYTES;
    Serial.printf("[UDP] Format Header C3MS: %lu Hz, %u-bit, %s\n",
                  (unsigned long)sampleRate, bits, channels == 2 ? "Stereo" : "Mono");
    return true;
  }
  return false;
}

// DRAM Static buffers: completely eliminates task stack overflow risk
static uint8_t playbackIn[I2S_WRITE_BYTES / 2];
static uint8_t playbackOut[I2S_WRITE_BYTES * 2];

static void playbackTask(void*) {
  for(;;){
    if (!i2sReady || !bufferStarted){ vTaskDelay(pdMS_TO_TICKS(10)); continue; }

    size_t want = sizeof(playbackIn);
    size_t n = ringRead(playbackIn, want);
    if (n==0){
      stats.underruns++;
      if (audioDataSemaphore) xSemaphoreTake(audioDataSemaphore, pdMS_TO_TICKS(20));
      else vTaskDelay(pdMS_TO_TICKS(4));
      continue;
    }

    const uint8_t *writeBuf = playbackIn;
    size_t writeLen = n;
    uint8_t volume = settings.volumePercent;

    if (streamFormat.bitsPerSample == 16) {
      if (volume < 100) {
        int16_t *samples = reinterpret_cast<int16_t*>(playbackIn);
        size_t sampleCount = n / sizeof(int16_t);
        for (size_t i = 0; i < sampleCount; ++i) {
          samples[i] = (int16_t)(((int32_t)samples[i] * volume) / 100);
        }
      }

      if (streamFormat.channels==1) {
        size_t samples = n / 2;
        int16_t *src = reinterpret_cast<int16_t*>(playbackIn);
        int16_t *dst = reinterpret_cast<int16_t*>(playbackOut);
        for (size_t i=0;i<samples;i++) { dst[i*2]=src[i]; dst[i*2+1]=src[i]; }
        writeBuf = playbackOut;
        writeLen = samples * 4;
      }
    }

    if (i2sMux) xSemaphoreTake(i2sMux, portMAX_DELAY);
    bool ready = i2sReady;
    size_t w=0;
    esp_err_t r = ready ? i2s_write(I2S_NUM_0,writeBuf,writeLen,&w,pdMS_TO_TICKS(40)) : ESP_FAIL;
    if (i2sMux) xSemaphoreGive(i2sMux);

    if (r==ESP_OK && w>0) stats.bytesPlayed += (streamFormat.channels==1) ? (w/2) : w;
    else vTaskDelay(pdMS_TO_TICKS(2));
  }
}

static uint8_t udpPacketBuf[UDP_MAX_PACKET_BYTES];

static void streamTask(void*) {
  for(;;){
    if (WiFi.status() != WL_CONNECTED) {
      stopUdpListener();
      endI2S();
      ringClear();
      if (settings.streamEnabled && !stopRequested && receiverState!=RX_WIFI_OFFLINE && receiverState!=RX_WIFI_CONNECTING && receiverState!=RX_UPDATING) {
        Serial.printf("[WIFI] Disconnected (status=%d)\n", (int)WiFi.status());
        setReceiverState(RX_WIFI_OFFLINE, "WiFi disconnected");
      }
      vTaskDelay(pdMS_TO_TICKS(500));
      continue;
    }

    if (!settings.streamEnabled || stopRequested) {
      stopUdpListener();
      endI2S();
      ringClear();
      if (receiverState != RX_STOPPED) setReceiverState(RX_STOPPED);
      vTaskDelay(pdMS_TO_TICKS(150));
      continue;
    }

    // Ensure UDP listener is bound to port
    if (!udpListening) {
      if (udpMux) xSemaphoreTake(udpMux, portMAX_DELAY);
      if (udpClient.begin(settings.udpPort)) {
        udpListening = true;
        Serial.printf("[UDP] Listening on port %u\n", settings.udpPort);
        setReceiverState(RX_IDLE);
      } else {
        stats.streamErrors++;
        setReceiverState(RX_ERROR, "UDP bind failed");
      }
      if (udpMux) xSemaphoreGive(udpMux);
      vTaskDelay(pdMS_TO_TICKS(50));
      continue;
    }

    // Process incoming UDP audio datagrams
    int packetSize = udpClient.parsePacket();
    if (packetSize > 0) {
      int len = udpClient.read(udpPacketBuf, sizeof(udpPacketBuf));
      if (len > 0) {
        size_t payloadOffset = 0;
        if (!streamFormat.valid || (len >= (int)C3_FORMAT_HEADER_BYTES && memcmp(udpPacketBuf, "C3MS", 4) == 0)) {
          if (parseC3HeaderIfPresent(udpPacketBuf, len, payloadOffset)) {
            if (!i2sReady || streamFormat.sampleRate != DEFAULT_SAMPLE_RATE) {
              beginI2S(streamFormat.sampleRate, streamFormat.channels, streamFormat.bitsPerSample);
            }
          } else if (!streamFormat.valid) {
            Serial.println("[UDP] Waiting for valid C3MS format header");
          }
        }

        if (!i2sReady) {
          beginI2S(streamFormat.sampleRate, streamFormat.channels, streamFormat.bitsPerSample);
        }

        const uint8_t *audioData = udpPacketBuf + payloadOffset;
        size_t audioLen = len - payloadOffset;

        if (audioLen > 0) {
          stats.bytesReceived += audioLen;
          stats.lastReceiveMs = millis();
          if (stats.sessionStartedMs == 0) stats.sessionStartedMs = millis();

          if (!ringWrite(audioData, audioLen)) {
            vTaskDelay(pdMS_TO_TICKS(1));
          }

          if (!bufferStarted && ringSize() >= targetPrebufferBytes()) {
            bufferStarted = true;
            setReceiverState(RX_STREAMING);
          }
        }
      }
    } else {
      // Packet timeout detection
      if (bufferStarted && (millis() - stats.lastReceiveMs > STREAM_READ_TIMEOUT_MS)) {
        bufferStarted = false;
        stats.sessionStartedMs = 0;
        setReceiverState(RX_IDLE);
        Serial.println("[UDP] Audio stream idle (silence/paused)");
      }
      vTaskDelay(pdMS_TO_TICKS(1));
    }
  }
}

static void startStreaming(){
  stopRequested = false;
  settings.streamEnabled = true;
  saveSettings();
  setReceiverState(RX_IDLE);
}

static void stopStreaming(){
  stopRequested = true;
  settings.streamEnabled = false;
  saveSettings();
  stopUdpListener();
  ringClear();
  endI2S();
  setReceiverState(RX_STOPPED);
}

static void reconnectStreaming(){
  stopRequested = true;
  stopUdpListener();
  ringClear();
  endI2S();
  delay(50);
  stopRequested = false;
  settings.streamEnabled = true;
  saveSettings();
  setReceiverState(RX_IDLE);
}

static String jsonEscape(const String &in){ String o; o.reserve(in.length()+8); for(size_t i=0;i<in.length();++i){ char c=in[i]; switch(c){ case'\\':o+="\\\\";break; case'"':o+="\\\"";break; case'\n':o+="\\n";break; case'\r':o+="\\r";break; case'\t':o+="\\t";break; default: if((uint8_t)c>=0x20)o+=c; } } return o; }

static String makeStatusJson(){
  String ip=(WiFi.status()==WL_CONNECTED)?WiFi.localIP().toString():"";
  String ssid=(WiFi.status()==WL_CONNECTED)?WiFi.SSID():"";
  int rssi=(WiFi.status()==WL_CONNECTED)?WiFi.RSSI():0;
  uint32_t sess=0; if (stats.sessionStartedMs>0 && (receiverState==RX_STREAMING||receiverState==RX_BUFFERING)) sess=(millis()-stats.sessionStartedMs)/1000UL;
  String r; r.reserve(1050);
  r+="{";
  r+="\"device\":\""+jsonEscape(DEVICE_NAME)+"\",";
  r+="\"version\":\""+String(FIRMWARE_VERSION)+"\",";
  r+="\"state\":\""+String(receiverStateName(receiverState))+"\",";
  r+="\"stateCode\":"+String((int)receiverState)+",";
  r+="\"mode\":\"UDP Stream\",";
  r+="\"streamEnabled\":"+String(settings.streamEnabled?"true":"false")+",";
  r+="\"wifiConnected\":"+String(WiFi.status()==WL_CONNECTED?"true":"false")+",";
  r+="\"ssid\":\""+jsonEscape(ssid)+"\",";
  r+="\"ip\":\""+jsonEscape(ip)+"\",";
  r+="\"hostname\":\""+String(MDNS_HOSTNAME)+".local\",";
  r+="\"rssi\":"+String(rssi)+",";
  r+="\"udpPort\":"+String(settings.udpPort)+",";
  r+="\"formatValid\":"+String(streamFormat.valid?"true":"false")+",";
  r+="\"sampleRate\":"+String(streamFormat.valid?streamFormat.sampleRate:0)+",";
  r+="\"channels\":"+String(streamFormat.valid?streamFormat.channels:0)+",";
  r+="\"bits\":"+String(streamFormat.valid?streamFormat.bitsPerSample:0)+",";
  r+="\"volume\":"+String(settings.volumePercent)+",";
  r+="\"bufferBytes\":"+String((uint32_t)ringSize())+",";
  r+="\"bufferPercent\":"+String(ringPercent())+",";
  r+="\"bytesReceived\":"+String(stats.bytesReceived)+",";
  r+="\"bytesPlayed\":"+String(stats.bytesPlayed)+",";
  r+="\"underruns\":"+String(stats.underruns)+",";
  r+="\"reconnects\":"+String(stats.reconnects)+",";
  r+="\"streamErrors\":"+String(stats.streamErrors)+",";
  r+="\"sessionSeconds\":"+String(sess)+",";
  r+="\"heap\":"+String(ESP.getFreeHeap())+",";
  r+="\"cpuTemp\":"+String(temperatureRead(), 1)+",";
  r+="\"flashSize\":"+String(ESP.getFlashChipSize())+",";
  r+="\"oled\":"+String(settings.oledEnabled?"true":"false")+",";
  r+="\"lastError\":\""+jsonEscape(stats.lastError)+"\"";
  r+="}";
  return r;
}

static String makeConfigJson(){
  String r; r.reserve(450);
  r+="{";
  r+="\"udpPort\":"+String(settings.udpPort)+",";
  r+="\"autoReconnect\":"+String(settings.autoReconnect?"true":"false")+",";
  r+="\"oled\":"+String(settings.oledEnabled?"true":"false")+",";
  r+="\"streamEnabled\":"+String(settings.streamEnabled?"true":"false")+",";
  r+="\"bufferMs\":"+String(settings.targetBufferMs)+",";
  r+="\"volume\":"+String(settings.volumePercent)+",";
  r+="\"i2s\":{\"bclk\":"+String(I2S_BCLK_PIN)+",\"lrclk\":"+String(I2S_LRCLK_PIN)+",\"dout\":"+String(I2S_DOUT_PIN)+"}";
  r+="}";  return r;
}

static void sendJson(int code, const String &body){ server.sendHeader("Cache-Control","no-store,no-cache,must-revalidate,max-age=0"); server.sendHeader("Pragma","no-cache"); server.send(code,"application/json",body); }
static bool requirePost(){ if (server.method()!=HTTP_POST){ sendJson(405,"{\"ok\":false,\"error\":\"POST required\"}"); return false; } return true; }

static String htmlPage(){
  return R"HTML(<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#000000">
<title>C3 Music Receiver (UDP)</title>
<style>
:root{--bg:#000;--surface:#0a0a0b;--surface2:#111113;--text:#f6f7f9;--muted:#8f9299;--line:rgba(255,255,255,.09);--line2:rgba(255,255,255,.16);--accent:#b9d2ff;--good:#bff6cb;--bad:#ffb7ae;--radius:24px}
*{box-sizing:border-box}html{background:#000;color-scheme:dark}
body{margin:0;min-height:100vh;background:#000;color:var(--text);font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;font-optical-sizing:auto;-webkit-font-smoothing:antialiased;text-rendering:optimizeLegibility;padding-bottom:94px}
button,input,select,textarea{font:inherit}button{touch-action:manipulation}
.small{font-size:12px;color:var(--muted);line-height:1.45}
.chip{display:inline-flex;align-items:center;gap:7px;padding:7px 11px;border:1px solid var(--line2);border-radius:999px;background:var(--surface2);font-size:10px;font-weight:800;letter-spacing:.055em;text-transform:uppercase;color:#ddd;white-space:nowrap}
.chip:before{content:"";width:6px;height:6px;border-radius:50%;background:#777}.chip.ok{color:var(--good);border-color:rgba(185,246,197,.18)}.chip.ok:before{background:var(--good)}.chip.bad{color:var(--bad);border-color:rgba(255,180,171,.2)}.chip.bad:before{background:var(--bad)}.chip.connecting{color:var(--accent);border-color:rgba(159,197,255,.2)}.chip.connecting:before{background:var(--accent)}
main{width:min(820px,100%);margin:auto;padding:20px 16px 108px}.tab{display:none}.tab.active{display:block;animation:uiFadeIn .22s ease-out}
@keyframes uiFadeIn{from{opacity:.72;transform:translateY(7px)}to{opacity:1;transform:none}}
.card{background:linear-gradient(180deg,#0b0b0b,#070707);border:1px solid rgba(255,255,255,.18);border-radius:var(--radius);padding:18px;margin-bottom:12px;box-shadow:0 0 0 1px rgba(255,255,255,.10),0 0 22px rgba(255,255,255,.085),0 12px 28px rgba(0,0,0,.28),inset 0 1px 0 rgba(255,255,255,.06);position:relative;overflow:hidden}.card:before{content:"";position:absolute;inset:0;border-radius:inherit;pointer-events:none;background:linear-gradient(135deg,rgba(255,255,255,.022),transparent 42%,rgba(255,255,255,.008))}
.card.tight{padding:14px}.section-title{display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:13px}
h2{font-size:17px;margin:0;font-weight:720;letter-spacing:-.015em}h3{font-size:14px;margin:0;font-weight:680}
.hero{font-size:31px;font-weight:800;letter-spacing:-.04em;margin:5px 0 7px;line-height:1.08}.muted{color:var(--muted)}
.format-line{font-size:13px;color:var(--muted);font-weight:600;letter-spacing:.012em}.info-strip{display:flex;justify-content:space-between;align-items:center;margin-top:17px;padding:12px 14px;border:1px solid var(--line);border-radius:15px;background:rgba(255,255,255,.018);font-size:12px;color:var(--muted)}.info-strip strong{color:#e9e9e9;font-size:13px}.row{display:flex;justify-content:space-between;align-items:center;gap:14px;min-height:42px;border-bottom:1px solid rgba(255,255,255,.065);padding:9px 0}.row:last-child{border-bottom:0}
.label{color:var(--muted);font-size:12px}.value{text-align:right;max-width:62%;overflow-wrap:anywhere;font-size:12px;color:#ddd}
.status-grid{display:grid;grid-template-columns:repeat(2,1fr);gap:11px;margin-top:12px}.stat{background:rgba(255,255,255,.015);border:1px solid var(--line);border-radius:17px;padding:14px;min-height:76px}.stat .k{font-size:10px;color:var(--muted);letter-spacing:.04em;text-transform:uppercase;margin-bottom:7px}.stat .v{font-size:18px;font-weight:760;letter-spacing:-.02em}
.buttons{display:flex;flex-wrap:wrap;gap:10px;margin-top:14px}
button.action{min-height:46px;border:1px solid transparent;border-radius:15px;padding:10px 16px;background:linear-gradient(180deg,#fff,#ededee);color:#000;font-weight:750;cursor:pointer;transition:transform .16s ease,background .16s ease,box-shadow .16s ease;box-shadow:0 7px 20px rgba(0,0,0,.28)}
button.action:hover{transform:translateY(-1px);box-shadow:0 10px 24px rgba(0,0,0,.34),0 0 0 1px rgba(255,255,255,.08)}button.action:active{transform:scale(.97)}
button.secondary{background:linear-gradient(180deg,#141416,#0d0d0e);color:#eee;border-color:rgba(255,255,255,.13)}button.danger{background:linear-gradient(180deg,#1a100f,#120908);color:#ffb4ab;border-color:rgba(255,180,171,.25)}#settings .card:last-child .buttons{flex-wrap:nowrap;gap:7px}#settings .card:last-child .buttons button.action{flex:1;min-width:0;min-height:42px;padding:8px 8px;font-size:12px}button:disabled{opacity:.48;cursor:not-allowed}
.field{margin:18px 0}.field label{display:block;color:var(--muted);font-size:11px;letter-spacing:.045em;text-transform:uppercase;margin:0 0 8px 2px}
input[type=text],input[type=number]{width:100%;height:48px;border:1px solid rgba(255,255,255,.13);outline:none;background:rgba(255,255,255,.02);color:var(--text);border-radius:15px;padding:0 13px;transition:border-color .16s ease,box-shadow .16s ease}input[type=text]:focus,input[type=number]:focus{border-color:rgba(185,210,255,.42);box-shadow:0 0 0 4px rgba(185,210,255,.055)}
input[type=range]{--volume:50%;width:100%;height:38px;margin:2px 0;appearance:none;background:transparent;accent-color:#fff;cursor:pointer}
input[type=range]::-webkit-slider-runnable-track{height:7px;background:linear-gradient(90deg,#f4f4f4 0,var(--volume),rgba(255,255,255,.12) var(--volume),rgba(255,255,255,.12) 100%);border-radius:99px}
input[type=range]::-webkit-slider-thumb{appearance:none;width:25px;height:25px;border-radius:50%;background:#fff;margin-top:-10px;border:2px solid #050505;box-shadow:0 3px 15px rgba(0,0,0,.4),0 0 0 3px rgba(255,255,255,.055);transition:transform .12s ease}input[type=range]:active::-webkit-slider-thumb{transform:scale(1.1)}
.volume-row{display:flex;align-items:center;gap:12px}.volume-row input{flex:1;min-width:0}.mute-btn{flex:0 0 auto;height:42px;min-width:78px;padding:0 13px;border:1px solid rgba(255,255,255,.13);border-radius:13px;background:#111113;color:#d8d8d8;font-size:12px;font-weight:750;cursor:pointer;transition:background .16s ease,transform .12s ease}.mute-btn:hover{border-color:rgba(255,255,255,.22)}.mute-btn:active{transform:scale(.96)}.mute-btn.muted{background:#17100f;color:#ffb4ab;border-color:rgba(255,180,171,.22)}
.slider-head{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:7px}.slider-value{font-size:20px;font-weight:800;letter-spacing:-.03em;color:#fff}
.switchrow{display:flex;justify-content:space-between;align-items:center;gap:14px;min-height:56px;padding:12px 0;border-bottom:1px solid var(--line)}.switchrow:last-child{border-bottom:0}.switchrow>span{font-size:13px;color:#e6e6e8}
.switch{appearance:none;flex:0 0 auto;width:50px;height:29px;border-radius:99px;background:#252528;border:1px solid #3a3a3e;position:relative;outline:none;cursor:pointer;transition:background .18s ease}
.switch:before{content:"";position:absolute;width:21px;height:21px;left:3px;top:3px;background:#858585;border-radius:50%;transition:transform .18s ease}
.switch:checked{background:#fff;border-color:#fff}.switch:checked:before{transform:translateX(21px);background:#000}
.settings-intro{margin:0 3px 14px;color:var(--muted);font-size:11px;letter-spacing:.02em}.settings-intro strong{color:#f2f2f4}
.file{width:100%;padding:13px;border:1px dashed var(--line2);border-radius:15px;background:rgba(255,255,255,.018);color:#bbb}.file::file-selector-button{border:1px solid var(--line2);background:#151515;color:#fff;border-radius:10px;padding:8px 11px;margin-right:9px;font-weight:650}
progress{width:100%;height:8px;border-radius:99px;accent-color:#fff;margin-top:12px}
nav{position:fixed;z-index:40;bottom:0;left:0;right:0;display:flex;justify-content:center;gap:6px;padding:9px 11px calc(9px + env(safe-area-inset-bottom));background:rgba(4,4,5,.91);backdrop-filter:blur(22px) saturate(120%);border-top:1px solid rgba(255,255,255,.08);box-shadow:0 -10px 32px rgba(0,0,0,.34)}
nav .nav-inner{width:min(540px,100%);display:flex;gap:7px}nav button{flex:1;min-height:50px;border:0;border-radius:16px;background:transparent;color:#777;font-weight:700;cursor:pointer;display:flex;align-items:center;justify-content:center;position:relative;transition:background .16s ease,color .16s ease}
nav button.active{background:linear-gradient(180deg,#18181b,#111113);color:#fff;box-shadow:inset 0 0 0 1px rgba(255,255,255,.075),0 6px 18px rgba(0,0,0,.22)}
nav .nav-inner button.active:after{content:"";position:absolute;bottom:5px;width:20px;height:2px;border-radius:2px;background:#c4d7ff;box-shadow:0 0 10px rgba(196,215,255,.25)}
nav .nav-inner button svg{width:30px;height:30px;fill:none;stroke:currentColor;stroke-width:1.65;stroke-linecap:round;stroke-linejoin:round}nav .nav-inner button:first-child svg{fill:currentColor;stroke:none;width:27px;height:27px}
.main-actions button.action{flex:1;min-width:0}.main-actions .icon-action{display:flex;align-items:center;justify-content:center;gap:0;padding:10px;background:#000;color:#fff;border:1px solid rgba(255,255,255,.14);box-shadow:none}.main-actions .icon-action:hover{background:#000;box-shadow:0 0 0 1px rgba(255,255,255,.12)}.main-actions .icon-action svg{width:22px;height:22px;fill:none;stroke:currentColor;stroke-width:2;stroke-linecap:round;stroke-linejoin:round}.main-actions .icon-action:first-child svg{fill:currentColor;stroke:none;width:24px;height:24px}.main-actions .icon-action:nth-child(1){color:#39ff88}.main-actions .icon-action:nth-child(2){color:#ff3b4d}.main-actions .icon-action:nth-child(3){color:#ffbf3f}#toast{position:fixed;z-index:80;left:50%;bottom:96px;transform:translate(-50%,14px);opacity:0;pointer-events:none;background:rgba(241,241,243,.97);color:#050505;border:1px solid rgba(255,255,255,.12);border-radius:14px;padding:11px 15px;font-size:13px;font-weight:700;box-shadow:0 16px 34px rgba(0,0,0,.48);backdrop-filter:blur(14px);transition:opacity .18s ease,transform .18s ease}
#toast.show{opacity:1;transform:translate(-50%,0)}.hidden{display:none!important}
.settings-icon{display:inline-flex;align-items:center;justify-content:center;width:20px;height:20px;color:var(--muted);flex:0 0 20px}.settings-icon svg{width:18px;height:18px;fill:none;stroke:currentColor;stroke-width:1.65;stroke-linecap:round;stroke-linejoin:round}
/* C3 Music typography system — S3-inspired, adapted for C3 */
:root{--font-ui:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;--fs-body:13px;--fs-small:12px;--fs-label:12px;--fs-value:12px;--fs-control:12px;--fs-section:17px;--fs-subsection:14px;--fs-hero:31px;--fw-regular:500;--fw-medium:600;--fw-section:720;--fw-control:700;--fw-strong:760;--fw-hero:800}
body,button,input,select,textarea{font-family:var(--font-ui);font-synthesis:none}
body{font-size:var(--fs-body);font-weight:var(--fw-regular);line-height:1.45;letter-spacing:0}
h2{font-size:var(--fs-section);font-weight:800;line-height:1.2;letter-spacing:-.015em;text-transform:uppercase}
h3{font-size:var(--fs-subsection);font-weight:var(--fw-section);line-height:1.25;letter-spacing:-.005em}
.hero{font-size:var(--fs-hero);font-weight:var(--fw-hero);line-height:1.08;letter-spacing:-.04em}
.small,.settings-intro,.format-line,.label,.field label,.switchrow>span,.stat .k,.info-strip span{font-size:var(--fs-small);font-weight:var(--fw-regular);line-height:1.45;opacity:.8}
.label,.field label{font-size:var(--fs-label);font-weight:var(--fw-medium);line-height:1.35}
.value{font-size:var(--fs-value);font-weight:var(--fw-medium);line-height:1.35}
.format-line{font-size:var(--fs-body);font-weight:var(--fw-medium);line-height:1.35;letter-spacing:.01em;opacity:.8}
.info-strip{font-size:var(--fs-small);font-weight:var(--fw-regular)}.info-strip strong{font-size:var(--fs-body);font-weight:var(--fw-strong)}
.stat .k{font-size:10px;font-weight:var(--fw-medium);letter-spacing:.05em}.stat .v{font-size:18px;font-weight:var(--fw-strong);line-height:1.15;letter-spacing:-.02em}
.chip{font-size:10px;font-weight:800;letter-spacing:.055em}
button.action,.mute-btn,.file::file-selector-button,.select-option{font-size:var(--fs-control);font-weight:var(--fw-control);line-height:1.2}
.field{font-size:var(--fs-body)}
.switchrow>span{font-size:var(--fs-body);font-weight:var(--fw-medium);line-height:1.35}
.select-button{font-size:var(--fs-body);font-weight:var(--fw-medium)}
.slider-value{font-size:20px;font-weight:var(--fw-hero);line-height:1;letter-spacing:-.03em}
#toast{font-size:var(--fs-body);font-weight:var(--fw-control);line-height:1.3}
.wifi-actions{flex-wrap:nowrap;gap:8px}.wifi-actions button.action{flex:1 1 0;min-width:0;min-height:40px;padding:8px 10px;font-size:11.5px;font-weight:var(--fw-control);border-radius:13px}.wifi-actions button.action:first-child{background:linear-gradient(180deg,#1a100f,#120908);color:rgba(255,180,171,.8);border-color:rgba(255,180,171,.20);box-shadow:none}.wifi-actions button.action:last-child{background:linear-gradient(180deg,#101610,#090d09);color:rgba(191,246,203,.8);border-color:rgba(191,246,203,.18);box-shadow:none}
@media(max-width:360px){.wifi-actions{gap:6px}.wifi-actions button.action{padding-left:7px;padding-right:7px;font-size:11px}}
</style>
</head>
<body>
<main>
<section class="tab active" id="now">
<div class="card"><div class="section-title"><h2>Now Playing</h2><span class="chip" id="deviceStatus">Loading</span></div><div class="hero" id="state">Connecting…</div><div class="format-line" id="format">Waiting for status</div><div class="info-strip"><span>Buffer</span><strong id="bufferText">—</strong></div>
<div class="card tight" style="margin:16px 0 0"><div class="slider-head"><h3>Volume</h3><span class="slider-value" id="volumeText">50%</span></div><div class="volume-row"><input id="volumeInput" type="range" min="0" max="100" value="50" oninput="volumePreview(this.value)" onchange="setVolume(this.value)"><button type="button" class="mute-btn" id="muteBtn" onclick="toggleMute()">Mute</button></div><div class="small">Output level</div></div>
<div class="buttons main-actions"><button class="action icon-action" aria-label="Start" title="Start" onclick="act('/api/stream/start')"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M8 5.5v13l10-6.5z"/></svg></button><button class="action secondary icon-action" aria-label="Stop" title="Stop" onclick="act('/api/stream/stop')"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M7 7h10v10H7z"/></svg></button><button class="action secondary icon-action" aria-label="Reconnect" title="Reconnect" onclick="act('/api/stream/reconnect')"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M19 8.5A7.5 7.5 0 0 0 5.6 6.1L4 8m0 0V4m0 4h4"/><path d="M5 15.5A7.5 7.5 0 0 0 18.4 17.9L20 16m0 0v4m0-4h-4"/></svg></button></div></div>
<div class="card"><div class="section-title"><h2>Stream</h2><span class="settings-icon" aria-hidden="true"><svg viewBox="0 0 24 24"><path d="M7 17.5 3.5 14a3.5 3.5 0 0 1 0-5l2-2a3.5 3.5 0 0 1 5 0l1.5 1.5M17 6.5 20.5 10a3.5 3.5 0 0 1 0 5l-2 2a3.5 3.5 0 0 1-5 0L12 15.5M8.5 15.5l7-7"/></svg></span></div><div class="row"><span class="label">Transport</span><span class="value" id="mode">UDP Stream</span></div><div class="row"><span class="label">Listening Port</span><span class="value" id="portDisplay">50005</span></div><div class="row"><span class="label">Session</span><span class="value" id="session">—</span></div></div>
<div class="card"><div class="section-title"><h2>Audio Health</h2><span class="settings-icon" aria-hidden="true"><svg viewBox="0 0 24 24"><path d="M3 12h4l2-4 3 8 2-4h7"/></svg></span></div><div class="status-grid"><div class="stat"><div class="k">Underruns</div><div class="v" id="underruns">0</div></div><div class="stat"><div class="k">Reconnects</div><div class="v" id="reconnects">0</div></div></div><div class="row" style="margin-top:8px"><span class="label">Last Error</span><span class="value" id="lastError">None</span></div></div>
</section>
<section class="tab" id="wifi"><div class="card"><div class="section-title"><h2>Wi‑Fi</h2><span class="settings-icon" aria-hidden="true"><svg viewBox="0 0 24 24"><path d="M4 9.5a12.5 12.5 0 0 1 16 0M7.2 13a7.5 7.5 0 0 1 9.6 0M10.3 16.2a2.8 2.8 0 0 1 3.4 0M12 19h.01"/></svg></span></div><div class="row"><span class="label">Status</span><span class="value" id="wifiStatus">—</span></div><div class="row"><span class="label">SSID</span><span class="value" id="ssid">—</span></div><div class="row"><span class="label">IP</span><span class="value" id="ip">—</span></div><div class="row"><span class="label">Hostname</span><span class="value">c3music.local</span></div><div class="row"><span class="label">Signal</span><span class="value" id="rssi">—</span></div><div class="buttons wifi-actions"><button class="action secondary" onclick="act('/api/wifi/reconnect')">Reconnect Wi‑Fi</button></div></div></section>
<section class="tab" id="settings">
<div class="card"><div class="section-title"><h2>Stream Settings</h2><span class="settings-icon" aria-hidden="true"><svg viewBox="0 0 24 24"><path d="M7 17.5 3.5 14a3.5 3.5 0 0 1 0-5l2-2a3.5 3.5 0 0 1 5 0l1.5 1.5M17 6.5 20.5 10a3.5 3.5 0 0 1 0 5l-2 2a3.5 3.5 0 0 1-5 0L12 15.5M8.5 15.5l7-7"/></svg></span></div>
<div class="field"><label>UDP Listening Port</label><input id="udpInput" type="number" min="1" max="65535" value="50005"></div>
<div class="field"><label>Target Buffer (ms)</label><input id="bufferInput" type="number" min="80" max="700" step="10"></div>
<div class="switchrow"><span>Automatic Stream Reconnect</span><input class="switch" id="autoreconnectInput" type="checkbox"></div><div class="buttons"><button class="action" onclick="saveCfg()">Save Settings</button></div></div>
<div class="card"><div class="section-title"><h2>Device Settings</h2><span class="settings-icon" aria-hidden="true"><svg viewBox="0 0 24 24"><rect x="6" y="4" width="12" height="16" rx="2"/><path d="M9 7h6M9 17h6"/></svg></span></div><div class="switchrow"><span>OLED Enabled</span><input class="switch" id="oledInput" type="checkbox" onchange="saveCfg()"></div><div class="row"><span class="label">Firmware</span><span class="value" id="version">—</span></div><div class="row"><span class="label">Free heap</span><span class="value" id="heap">—</span></div><div class="row"><span class="label">CPU temperature</span><span class="value" id="cpuTemp">—</span></div></div>
<div class="card"><div class="section-title"><h2>Firmware OTA</h2><span class="settings-icon" aria-hidden="true"><svg viewBox="0 0 24 24"><path d="M12 16V4M7.5 8.5 12 4l4.5 4.5M6 15v4h12v-4"/></svg></span></div><p class="small">Select a .bin file. Streaming will stop during update.</p><input id="firmware" class="file" type="file" accept=".bin"><div class="buttons"><button class="action" id="otaBtn" onclick="uploadFw()">Install Firmware</button></div><progress id="otaProg" class="hidden" value="0" max="100"></progress><div class="small" id="otaTxt"></div></div>
<div class="card"><div class="section-title"><h2>Maintenance</h2><span class="settings-icon" aria-hidden="true"><svg viewBox="0 0 24 24"><path d="M12 8.7a3.3 3.3 0 1 0 0 6.6 3.3 3.3 0 0 0 0-6.6Z"/><path d="m19 13.4 1.3 1-1.8 3.1-1.6-.7a7.5 7.5 0 0 1-2.5 1.4L14.1 20h-3.6l-.3-1.8a7.5 7.5 0 0 1-2.5-1.4l-1.6.7-1.8-3.1 1.3-1a7.4 7.4 0 0 1 0-2.8l-1.3-1 1.8-3.1 1.6.7a7.5 7.5 0 0 1 2.5-1.4L10.5 4h3.6l.3 1.8a7.5 7.5 0 0 1 2.5 1.4l1.6-.7 1.8 3.1-1.3 1a7.4 7.4 0 0 1 0 2.8Z"/></svg></span></div><div class="buttons"><button class="action secondary" onclick="act('/api/system/clear-stats')">Clear Stats</button><button class="action secondary" onclick="act('/api/system/reboot')">Restart</button><button class="action danger" onclick="if(confirm('Factory Reset?'))act('/api/system/factory-reset')">Factory Reset</button></div></div>
</section></main>
<nav><div class="nav-inner"><button class="active" data-tab="now" aria-label="Now Playing" title="Now Playing"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M9 6.5v11l8.5-5.5z"/></svg></button><button data-tab="wifi" aria-label="Wi-Fi" title="Wi-Fi"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 9.5a12.5 12.5 0 0 1 16 0M7.2 13a7.5 7.5 0 0 1 9.6 0M10.3 16.2a2.8 2.8 0 0 1 3.4 0M12 19h.01"/></svg></button><button data-tab="settings" aria-label="Settings" title="Settings"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 8.7a3.3 3.3 0 1 0 0 6.6 3.3 3.3 0 0 0 0-6.6Z"/><path d="M19 13.4a7.4 7.4 0 0 0 0-2.8l1.5-1.1-1.8-3.1-1.8.7a7.5 7.5 0 0 0-2.4-1.4L14.3 4h-3.6l-.3 1.7A7.5 7.5 0 0 0 8 7.1l-1.8-.7-1.8 3.1L6 10.6a7.4 7.4 0 0 0 0 2.8l-1.5 1.1 1.8 3.1 1.8-.7a7.5 7.5 0 0 0 2.4 1.4l.3 1.7h3.6l.3-1.7a7.5 7.5 0 0 0 2.4-1.4l1.8.7 1.8-3.1Z"/></svg></button></div></nav><div id="toast"></div>
<script>
const $=id=>document.getElementById(id);let cfgLoaded=false;let lastVolume=50;
function toast(t){const x=$('toast');x.textContent=t;x.classList.add('show');clearTimeout(window.__toastTimer);window.__toastTimer=setTimeout(()=>x.classList.remove('show'),2500)}
function text(id,v){const e=$(id);if(e)e.textContent=v}
function time(s){s=Number(s||0);const h=Math.floor(s/3600),m=Math.floor(s%3600/60),q=s%60;return[h,m,q].map(x=>String(x).padStart(2,'0')).join(':')}
function updateVolumeUI(v){const n=Math.max(0,Math.min(100,Number(v)||0));const input=$('volumeInput');if(input)input.style.setProperty('--volume',n+'%');text('volumeText',n+'%');const b=$('muteBtn');if(b){const muted=n===0;b.textContent=muted?'Unmute':'Mute';b.classList.toggle('muted',muted)}}
let volumeTimer=0;let volumeChanging=false;let volumeRequest=0;
function volumePreview(v){const n=Math.max(0,Math.min(100,Number(v)||0));if(n>0)lastVolume=n;updateVolumeUI(n);volumeChanging=true;clearTimeout(volumeTimer);volumeTimer=setTimeout(()=>sendVolume(n,false),40)}
function toggleMute(){const input=$('volumeInput');if(!input)return;const current=Number(input.value)||0;const target=current>0?0:(lastVolume>0?lastVolume:50);input.value=target;updateVolumeUI(target);setVolume(target)}
async function sendVolume(v,commit){const n=Math.max(0,Math.min(100,Number(v)||0));const requestId=++volumeRequest;try{const body=new URLSearchParams({value:String(n),commit:commit?'1':'0'});const r=await fetch('/api/volume',{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded'},body});const d=await r.json();if(commit&&requestId===volumeRequest)toast(d.ok?'Volume '+n+'%':(d.error||'Volume change failed'))}catch(e){if(commit&&requestId===volumeRequest)toast('Volume change failed')}}
function setVolume(v){const n=Math.max(0,Math.min(100,Number(v)||0));if(n>0)lastVolume=n;updateVolumeUI(n);volumeChanging=false;clearTimeout(volumeTimer);sendVolume(n,true)}
function apply(d){
 text('state',d.state||'—');text('deviceStatus',d.state||'—');
 const formatActive=!!d.formatValid && (d.state==='Buffering'||d.state==='Streaming');
 text('format',formatActive?((d.sampleRate||0)+' Hz · '+(d.bits||0)+'-bit · '+(d.channels===2?'Stereo':'Mono')):'UDP Audio Waiting');
 text('mode',d.mode||'UDP Stream');text('portDisplay',d.udpPort||50005);text('session',time(d.sessionSeconds));text('underruns',d.underruns||0);text('reconnects',d.reconnects||0);text('lastError',d.lastError||'None');
 const vol=Math.max(0,Math.min(100,Number(d.volume??50)||0)),vi=$('volumeInput');if(vi&&document.activeElement!==vi){vi.value=vol;updateVolumeUI(vol)}
 text('wifiStatus',d.wifiConnected?'Connected':'Disconnected');text('ssid',d.ssid||'—');text('ip',d.ip||'—');text('rssi',d.wifiConnected?(d.rssi+' dBm'):'—');text('version',d.version||'—');text('heap',d.heap?(Math.round(d.heap/1024)+' KB'):'—');text('cpuTemp',d.cpuTemp!==undefined&&d.cpuTemp!==null?(Number(d.cpuTemp).toFixed(1)+' °C'):'—');
 const p=Number(d.bufferPercent||0);text('bufferText',p+'% · '+(d.bufferBytes||0)+' bytes');const c=$('deviceStatus');c.className='chip '+(d.state==='Streaming'?'ok':(d.state==='Stopped'||d.state==='Error'||d.state==='WiFi offline'?'bad':(d.state==='Connecting'?'connecting':'')))
}
let pollBusy=false;let pollTimer=0;async function poll(){if(pollBusy)return;pollBusy=true;try{const r=await fetch('/api/status',{cache:'no-store'});if(!r.ok)throw 0;apply(await r.json());if(!cfgLoaded)await loadCfg()}catch(e){text('state','Web lost');text('deviceStatus','Offline');$('deviceStatus').className='chip bad'}finally{pollBusy=false}}function schedulePoll(){clearTimeout(pollTimer);pollTimer=setTimeout(()=>{poll();schedulePoll()},document.visibilityState==='visible'?1000:1500)}
async function loadCfg(){try{const d=await(await fetch('/api/config',{cache:'no-store'})).json();$('udpInput').value=d.udpPort||50005;$('bufferInput').value=d.bufferMs||250;$('autoreconnectInput').checked=!!d.autoReconnect;$('oledInput').checked=!!d.oled;const v=Math.max(0,Math.min(100,Number(d.volume??50)||0));$('volumeInput').value=v;updateVolumeUI(v);cfgLoaded=true}catch(e){}}
async function act(url){try{const r=await fetch(url,{method:'POST'}),d=await r.json();toast(d.ok?'Done':(d.error||'Failed'));setTimeout(poll,300)}catch(e){toast('Request failed')}}
async function saveCfg(){const body=new URLSearchParams({udpPort:$('udpInput').value,bufferMs:$('bufferInput').value,autoReconnect:$('autoreconnectInput').checked?'1':'0',oled:$('oledInput').checked?'1':'0'});try{const r=await fetch('/api/config',{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded'},body});const d=await r.json();toast(d.ok?'Saved':'Save failed');cfgLoaded=false;setTimeout(poll,500)}catch(e){toast('Save failed')}}
function uploadFw(){const f=$('firmware').files[0];if(!f){toast('Choose .bin first');return}if(!confirm('Install '+f.name+'?'))return;const xhr=new XMLHttpRequest(),form=new FormData();form.append('firmware',f);$('otaProg').classList.remove('hidden');$('otaProg').value=0;$('otaTxt').textContent='Uploading…';$('otaBtn').disabled=true;xhr.upload.onprogress=e=>{if(e.lengthComputable){const p=Math.round(e.loaded/e.total*100);$('otaProg').value=p;$('otaTxt').textContent='Uploading '+p+'%'}};xhr.onload=()=>{$('otaBtn').disabled=false;if(xhr.status===200){$('otaProg').value=100;$('otaTxt').textContent='Update accepted. Restarting…';toast('Firmware update successful')}else{$('otaTxt').textContent='Update failed: '+xhr.responseText;toast('OTA failed')}};xhr.onerror=()=>{$('otaBtn').disabled=false;$('otaTxt').textContent='Upload failed';toast('OTA failed')};xhr.open('POST','/api/ota');xhr.send(form)}
const navButtons=Array.from(document.querySelectorAll('nav button'));const tabs=Array.from(document.querySelectorAll('.tab'));function showTab(index,direction=0){if(index<0||index>=navButtons.length)return;navButtons.forEach(x=>x.classList.remove('active'));tabs.forEach(x=>x.classList.remove('active','from-left','from-right'));const b=navButtons[index],panel=$(b.dataset.tab);b.classList.add('active');if(panel){panel.classList.add('active');if(direction<0)panel.classList.add('from-right');else if(direction>0)panel.classList.add('from-left')}window.scrollTo({top:0,behavior:'smooth'})}navButtons.forEach((b,i)=>b.onclick=()=>showTab(i,0));
poll();schedulePoll();document.addEventListener('visibilitychange',schedulePoll);
</script></body></html>)HTML";
}

static bool otaUploadFailed=false; static String otaUploadError;
static void handleFirmwareUpload(){
  HTTPUpload &up=server.upload();
  if (up.status==UPLOAD_FILE_START){ otaUploadFailed=false; otaUploadError=""; stopRequested=true; stopUdpListener(); ringClear(); endI2S(); setReceiverState(RX_UPDATING); size_t sz=UPDATE_SIZE_UNKNOWN; if (!Update.begin(sz,U_FLASH)){ otaUploadFailed=true; otaUploadError=Update.errorString(); Serial.printf("[OTA] Begin failed: %s\n",otaUploadError.c_str()); } else Serial.printf("[OTA] Start: %s\n",up.filename.c_str()); }
  else if (up.status==UPLOAD_FILE_WRITE){ if (!otaUploadFailed){ size_t w=Update.write(up.buf,up.currentSize); if (w!=up.currentSize){ otaUploadFailed=true; otaUploadError=Update.errorString(); Serial.printf("[OTA] Write failed: %s\n",otaUploadError.c_str()); } } }
  else if (up.status==UPLOAD_FILE_END){ if (!otaUploadFailed){ if (!Update.end(true)){ otaUploadFailed=true; otaUploadError=Update.errorString(); Serial.printf("[OTA] End failed: %s\n",otaUploadError.c_str()); } else Serial.printf("[OTA] Success: %u bytes\n",up.totalSize); } }
  else if (up.status==UPLOAD_FILE_ABORTED){ otaUploadFailed=true; otaUploadError="Upload aborted"; Update.abort(); }
}

static void setupWebServer(){
  server.on("/",HTTP_GET,[]{ server.sendHeader("Cache-Control","public,max-age=300,must-revalidate"); server.sendHeader("Vary","Accept-Encoding"); server.send(200,"text/html; charset=utf-8",htmlPage()); });
  server.on("/api/status",HTTP_GET,[]{ sendJson(200,makeStatusJson()); });
  server.on("/api/config",HTTP_GET,[]{ sendJson(200,makeConfigJson()); });
  server.on("/api/config",HTTP_POST,[]{
    if (!requirePost()) return;
    uint32_t up=server.arg("udpPort").toInt();
    if (up<1||up>65535){ sendJson(400,"{\"ok\":false,\"error\":\"Invalid port\"}"); return; }
    settings.udpPort=(uint16_t)up;
    uint32_t bm=server.arg("bufferMs").toInt(); if (bm<80||bm>700){ sendJson(400,"{\"ok\":false,\"error\":\"Invalid buffer\"}"); return; }
    settings.autoReconnect=(server.arg("autoReconnect")=="1"); settings.oledEnabled=(server.arg("oled")=="1"); settings.targetBufferMs=(uint16_t)bm;
    if (oledAvailable) {
      if (settings.oledEnabled) {
        oledStreamingDisabled = false;
        oled.setPowerSave(0);
        oled.clearBuffer();
        oled.sendBuffer();
      } else {
        oledStreamingDisabled = false;
        oled.setPowerSave(1);
      }
    }
    saveSettings(); reconnectStreaming(); sendJson(200,"{\"ok\":true}");
  });
  server.on("/api/volume",HTTP_POST,[]{
    if(!requirePost()) return;
    int v=server.arg("value").toInt();
    if(v<0||v>100){ sendJson(400,"{\"ok\":false,\"error\":\"Invalid volume\"}"); return; }
    settings.volumePercent=(uint8_t)v;
    if(server.arg("commit")=="1") preferences.putUChar("volume", settings.volumePercent);
    sendJson(200,"{\"ok\":true,\"volume\":"+String(settings.volumePercent)+"}");
  });
  server.on("/api/stream/start",HTTP_POST,[]{ if(!requirePost())return; startStreaming(); sendJson(200,"{\"ok\":true}"); });
  server.on("/api/stream/stop",HTTP_POST,[]{ if(!requirePost())return; stopStreaming(); sendJson(200,"{\"ok\":true}"); });
  server.on("/api/stream/reconnect",HTTP_POST,[]{ if(!requirePost())return; reconnectStreaming(); sendJson(200,"{\"ok\":true}"); });
  server.on("/api/wifi/reconnect",HTTP_POST,[]{ if(!requirePost())return; WiFi.disconnect(false,false); stopUdpListener(); mdnsStarted=false; lastWifiAttemptMs=0; connectWifiIfNeeded(); sendJson(200,"{\"ok\":true}"); });
  server.on("/api/system/clear-stats",HTTP_POST,[]{ if(!requirePost())return; stats=RuntimeStats(); sendJson(200,"{\"ok\":true}"); });
  server.on("/api/system/reboot",HTTP_POST,[]{ if(!requirePost())return; sendJson(200,"{\"ok\":true,\"message\":\"Restarting\"}"); delay(250); ESP.restart(); });
  server.on("/api/system/factory-reset",HTTP_POST,[]{ if(!requirePost())return; resetSettings(); sendJson(200,"{\"ok\":true,\"message\":\"Reset; restarting\"}"); delay(250); ESP.restart(); });
  server.on("/api/ota",HTTP_POST,[]{ if (otaUploadFailed){ String b="{\"ok\":false,\"error\":\""+jsonEscape(otaUploadError)+"\"}"; sendJson(500,b); stopRequested=false; settings.streamEnabled=true; setReceiverState(RX_IDLE); } else { sendJson(200,"{\"ok\":true,\"message\":\"Firmware written; restarting\"}"); delay(700); ESP.restart(); } }, handleFirmwareUpload);
  server.onNotFound([](){ sendJson(404,"{\"ok\":false,\"error\":\"Not found\"}"); });
  server.begin(); Serial.println("[WEB] HTTP server started");
}

void setup(){
  Serial.begin(115200); delay(400);
  Serial.println(); Serial.println("============================================================");
  Serial.printf("%s v%s\n",DEVICE_NAME,FIRMWARE_VERSION);
  Serial.println("High-Performance UDP Audio Stream Receiver (Port 50005)");
  Serial.println("============================================================");
  loadSettings();
  audioDataSemaphore=xSemaphoreCreateBinary();
  i2sMux=xSemaphoreCreateMutex();
  udpMux=xSemaphoreCreateMutex();
  displayBegin(); setReceiverState(RX_BOOTING);
  WiFi.persistent(false); WiFi.mode(WIFI_STA); WiFi.setSleep(false); WiFi.setAutoReconnect(true);
  setupWebServer();
  BaseType_t ok1=xTaskCreate(playbackTask,"i2sPlayback",4096,nullptr,3,&playbackTaskHandle);
  BaseType_t ok2=xTaskCreate(streamTask,"udpStream",6144,nullptr,2,&streamTaskHandle);
  if (ok1!=pdPASS||ok2!=pdPASS) setReceiverState(RX_ERROR,"Task creation failed");
  connectWifiIfNeeded(); displayUpdate();
}

void loop(){
  server.handleClient();
  if (WiFi.status()!=WL_CONNECTED){
    mdnsStarted=false;
    if (receiverState!=RX_WIFI_OFFLINE&&receiverState!=RX_WIFI_CONNECTING&&receiverState!=RX_UPDATING&&settings.streamEnabled){
      Serial.printf("[WIFI] Disconnected (status=%d)\n",(int)WiFi.status());
      setReceiverState(RX_WIFI_OFFLINE);
    }
    connectWifiIfNeeded();
  }
  else beginMdnsIfNeeded();
  displayUpdate();
  if (millis()-lastStatusRefreshMs>=STATUS_REFRESH_MS){
    lastStatusRefreshMs=millis();
    uint32_t target=targetPrebufferBytes();
    if (receiverState==RX_BUFFERING&&bufferStarted&&ringSize()>=(target/2)) setReceiverState(RX_STREAMING);
  }
  delay(2);
}
