/*
  C3 Music Receiver
  ESP32-C3 + SSD1306 72x40 OLED + UDA1334A I2S DAC
  TCP PCM (50005) primary, HTTP WAV (8080) fallback
  OLED: SDA 5, SCL 6
  I2S: BCLK 4, LRCLK 7, DOUT 10
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

static const char WIFI_SSID[] = "GFiber_2.4_Coverage_AECD9";
static const char WIFI_PASSWORD[] = "006BF4FD";
static const char PHONE_HOST[] = "192.168.254.119";

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
static constexpr uint32_t STREAM_READ_TIMEOUT_MS = 15000;
static constexpr uint32_t WAV_PARSE_MAX_BYTES = 4096;

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
  int lastHttpStatus = 0;
  String lastError;
};

struct Settings {
  String phoneHost;
  uint16_t tcpPort = TCP_DEFAULT_PORT;
  uint16_t httpPort = HTTP_DEFAULT_PORT;
  StreamMode preferredMode = STREAM_MODE_TCP;
  bool autoFallback = true, autoReconnect = true, oledEnabled = true, streamEnabled = true;
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
static volatile bool i2sReady = false;
static volatile bool bufferStarted = false;
static uint8_t audioRing[AUDIO_RING_BYTES];
static volatile size_t ringReadIndex = 0, ringWriteIndex = 0, ringCount = 0;
static portMUX_TYPE ringMux = portMUX_INITIALIZER_UNLOCKED;
static SemaphoreHandle_t audioDataSemaphore = nullptr;
static SemaphoreHandle_t i2sMux = nullptr;
static SemaphoreHandle_t streamClientMux = nullptr;
static TaskHandle_t streamTaskHandle = nullptr, playbackTaskHandle = nullptr;
static uint32_t lastWifiAttemptMs = 0, lastStreamAttemptMs = 0, lastOledRefreshMs = 0, lastStatusRefreshMs = 0;
static bool mdnsStarted = false;

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
  if (settings.phoneHost.length() == 0) settings.phoneHost = PHONE_HOST;
  // Migrate the previous built-in default without overwriting a user-configured host.
  if (settings.phoneHost == "192.168.1.100") {
    settings.phoneHost = PHONE_HOST;
    preferences.putString("host", settings.phoneHost);
  }
  if (settings.tcpPort == 0) settings.tcpPort = TCP_DEFAULT_PORT;
  if (settings.httpPort == 0) settings.httpPort = HTTP_DEFAULT_PORT;
  if (settings.targetBufferMs < 80) settings.targetBufferMs = 80;
  if (settings.targetBufferMs > 700) settings.targetBufferMs = 700;
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
  preferences.clear(); preferences.end();
  settings.phoneHost = PHONE_HOST;
  settings.tcpPort = TCP_DEFAULT_PORT;
  settings.httpPort = HTTP_DEFAULT_PORT;
  settings.preferredMode = STREAM_MODE_TCP;
  settings.autoFallback = true; settings.autoReconnect = true;
  settings.oledEnabled = true; settings.streamEnabled = true;
  settings.targetBufferMs = 250;
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

static const char* streamModeName(StreamMode m) { return m == STREAM_MODE_HTTP ? "HTTP WAV" : "TCP PCM"; }

static void setReceiverState(ReceiverState value, const String &error = "") {
  receiverState = value;
  if (error.length() > 0) { stats.lastError = error; Serial.printf("[STATE] %s: %s\n", receiverStateName(value), error.c_str()); }
  else { Serial.printf("[STATE] %s\n", receiverStateName(value)); }
}

static bool ringWrite(const uint8_t *data, size_t length) {
  if (!data || length == 0) return true;
  bool ok = true;
  portENTER_CRITICAL(&ringMux);
  size_t freeBytes = AUDIO_RING_BYTES - ringCount;
  if (length > freeBytes) { length = freeBytes; ok = false; }
  size_t first = min(length, AUDIO_RING_BYTES - ringWriteIndex);
  memcpy(audioRing + ringWriteIndex, data, first);
  size_t second = length - first;
  if (second > 0) memcpy(audioRing, data + first, second);
  ringWriteIndex = (ringWriteIndex + length) % AUDIO_RING_BYTES;
  ringCount += length;
  portEXIT_CRITICAL(&ringMux);
  if (length > 0 && audioDataSemaphore) xSemaphoreGive(audioDataSemaphore);
  return ok;
}

static size_t ringRead(uint8_t *out, size_t maxLen) {
  if (!out || maxLen == 0) return 0;
  portENTER_CRITICAL(&ringMux);
  size_t local = ringCount;
  size_t len = min(maxLen, local);
  if (len > 0) {
    size_t first = min(len, AUDIO_RING_BYTES - ringReadIndex);
    memcpy(out, audioRing + ringReadIndex, first);
    size_t second = len - first;
    if (second > 0) memcpy(out + first, audioRing, second);
    ringReadIndex = (ringReadIndex + len) % AUDIO_RING_BYTES;
    ringCount -= len;
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

static size_t ringSize() { portENTER_CRITICAL(&ringMux); size_t v = ringCount; portEXIT_CRITICAL(&ringMux); return v; }
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
    oled.drawFrame(0,33,OLED_WIDTH,7);
    uint8_t w = (uint8_t)(((OLED_WIDTH-2)*ringPercent())/100);
    if (w>0) oled.drawBox(1,34,w,5);
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

static bool connectStreamHost(uint16_t port) {
  String host = settings.phoneHost;
  host.trim();
  if (host.length()==0) return false;

  if (streamClientMux) xSemaphoreTake(streamClientMux, portMAX_DELAY);
  streamClient.stop();

  IPAddress ip;
  bool resolved = false;

  if (ip.fromString(host)) {
    resolved = true;
  } else if (host.endsWith(".local")) {
    ip = MDNS.queryHost(host, 2000);
    resolved = (ip != IPAddress(0,0,0,0));
  } else {
    resolved = WiFi.hostByName(host.c_str(), ip);
  }

  bool connected = false;
  if (resolved) {
    Serial.printf("[STREAM] Connecting to %s:%u\n", ip.toString().c_str(), port);
    connected = streamClient.connect(ip, port);
  } else {
    Serial.printf("[STREAM] Connecting to host %s:%u\n", host.c_str(), port);
    connected = streamClient.connect(host.c_str(), port);
  }

  if (streamClientMux) xSemaphoreGive(streamClientMux);
  return connected;
}

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

static bool supportedPcmFormat(const StreamFormat &f) {
  return f.audioFormat==1 && f.bitsPerSample==16 && (f.channels==1||f.channels==2) && f.sampleRate>=8000 && f.sampleRate<=96000;
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
  if (bytes > AUDIO_RING_BYTES - 4096) bytes = AUDIO_RING_BYTES - 4096;
  return (uint32_t)bytes;
}

static void stopStreamClient() {
  if (streamClientMux) xSemaphoreTake(streamClientMux, portMAX_DELAY);
  streamClient.stop();
  if (streamClientMux) xSemaphoreGive(streamClientMux);
}

static bool beginI2S(uint32_t sr, uint16_t ch, uint16_t bits) {
  if (bits!=16 || (ch!=1 && ch!=2)) return false;
  endI2S();
  i2s_config_t cfg = {
    .mode = (i2s_mode_t)(I2S_MODE_MASTER|I2S_MODE_TX),
    .sample_rate = sr,
    .bits_per_sample = I2S_BITS_PER_SAMPLE_16BIT,
    .channel_format = I2S_CHANNEL_FMT_RIGHT_LEFT,
    .communication_format = I2S_COMM_FORMAT_STAND_I2S,
    .intr_alloc_flags = ESP_INTR_FLAG_LEVEL1,
    .dma_buf_count = 8, .dma_buf_len = 256,
    .use_apll = false, .tx_desc_auto_clear = true, .fixed_mclk = I2S_PIN_NO_CHANGE
  };
  if (i2s_driver_install(I2S_NUM_0,&cfg,0,nullptr)!=ESP_OK) return false;
  i2s_pin_config_t pin = { I2S_BCLK_PIN, I2S_LRCLK_PIN, I2S_DOUT_PIN, I2S_PIN_NO_CHANGE, I2S_PIN_NO_CHANGE };
  if (i2s_set_pin(I2S_NUM_0,&pin)!=ESP_OK) { endI2S(); return false; }
  if (i2s_set_clk(I2S_NUM_0,sr,I2S_BITS_PER_SAMPLE_16BIT,I2S_CHANNEL_STEREO)!=ESP_OK) { endI2S(); return false; }
  i2sReady=true;
  Serial.printf("[I2S] %lu Hz, %u-bit, %s, BCLK=%d WS=%d DOUT=%d\n", (unsigned long)sr, bits, ch==2?"Stereo":"Mono", I2S_BCLK_PIN, I2S_LRCLK_PIN, I2S_DOUT_PIN);
  return true;
}

static bool readExact(WiFiClient &c, uint8_t *buf, size_t len, uint32_t to) {
  size_t got=0; uint32_t t=millis();
  while (got<len && !stopRequested) {
    int av=c.available();
    if (av>0) { size_t w=min((size_t)av,len-got); int n=c.read(buf+got,w); if (n>0){got+=n;t=millis();} }
    else { if (!c.connected() || millis()-t>to) return false; delay(1); }
  }
  return got==len;
}

static bool readLine(WiFiClient &c, String &line, uint32_t to) {
  line=""; uint32_t t=millis();
  while (!stopRequested) {
    if (c.available()) { char ch=(char)c.read(); line+=ch; if (line.endsWith(String("\r")+"\n")) return true; if (line.length()>1024) return false; t=millis(); }
    else { if (!c.connected() || millis()-t>to) return false; delay(1); }
  }
  return false;
}

static bool parseHttpHeaders() {
  String line;
  if (!readLine(streamClient,line,HTTP_HEADER_TIMEOUT_MS)) { stats.lastError="HTTP no status"; return false; }
  line.trim(); Serial.printf("[HTTP] %s\n",line.c_str());
  if (!line.startsWith("HTTP/") || line.indexOf(" 200")<0) { stats.lastError="HTTP "+line; return false; }
  stats.lastHttpStatus=200;
  while (!stopRequested) {
    if (!readLine(streamClient,line,HTTP_HEADER_TIMEOUT_MS)) { stats.lastError="HTTP header timeout"; return false; }
    if (line==String("\r")+"\n") return true;
    line.trim(); if (line.length()>0) Serial.printf("[HTTP] %s\n",line.c_str());
  }
  return false;
}

static uint32_t readLe32(const uint8_t *p){ return (uint32_t)p[0]|((uint32_t)p[1]<<8)|((uint32_t)p[2]<<16)|((uint32_t)p[3]<<24); }
static uint16_t readLe16(const uint8_t *p){ return p[0]|(p[1]<<8); }

static bool parseWavHeader(StreamFormat &f) {
  uint8_t riff[12];
  if (!readExact(streamClient,riff,sizeof(riff),HTTP_HEADER_TIMEOUT_MS)) { stats.lastError="WAV header missing"; return false; }
  if (memcmp(riff,"RIFF",4)!=0 || memcmp(riff+8,"WAVE",4)!=0) { stats.lastError="Not RIFF/WAV"; return false; }
  bool fmtOk=false,dataOk=false; size_t sc=12;
  while (!stopRequested && sc<WAV_PARSE_MAX_BYTES) {
    uint8_t hd[8];
    if (!readExact(streamClient,hd,sizeof(hd),HTTP_HEADER_TIMEOUT_MS)) { stats.lastError="WAV chunk timeout"; return false; }
    sc+=sizeof(hd);
    uint32_t sz=readLe32(hd+4);
    char nm[5]={ (char)hd[0],(char)hd[1],(char)hd[2],(char)hd[3],0 };
    if (memcmp(hd,"fmt ",4)==0) {
      if (sz<16||sz>64){ stats.lastError="Bad WAV fmt"; return false; }
      uint8_t buf[64];
      if (!readExact(streamClient,buf,sz,HTTP_HEADER_TIMEOUT_MS)){ stats.lastError="WAV fmt timeout"; return false; }
      sc+=sz;
      f.audioFormat=readLe16(buf+0); f.channels=readLe16(buf+2); f.sampleRate=readLe32(buf+4); f.bitsPerSample=readLe16(buf+14);
      f.valid=supportedPcmFormat(f);
      Serial.printf("[WAV] fmt=%u ch=%u rate=%lu bits=%u\n",f.audioFormat,f.channels,(unsigned long)f.sampleRate,f.bitsPerSample);
      if (!f.valid){ stats.lastError="Unsupported WAV PCM"; return false; }
      fmtOk=true;
    } else if (memcmp(hd,"data",4)==0) { dataOk=true; Serial.printf("[WAV] data chunk (%lu bytes)\n",(unsigned long)sz); break; }
    else {
      uint8_t disc[128]; uint32_t rem=sz;
      while (rem>0){ size_t p=min((uint32_t)sizeof(disc),rem); if (!readExact(streamClient,disc,p,HTTP_HEADER_TIMEOUT_MS)){ stats.lastError="WAV skip timeout"; return false; } rem-=p; sc+=p; }
      if (sz&1U){ uint8_t pad; if (!readExact(streamClient,&pad,1,HTTP_HEADER_TIMEOUT_MS)){ stats.lastError="WAV pad timeout"; return false; } sc++; }
    }
  }
  if (!fmtOk||!dataOk){ stats.lastError="WAV chunks invalid"; return false; }
  return true;
}

static bool connectRawTcp() {
  setReceiverState(RX_CONNECTING);
  streamFormat.sampleRate=DEFAULT_SAMPLE_RATE; streamFormat.channels=DEFAULT_CHANNELS; streamFormat.bitsPerSample=DEFAULT_BITS_PER_SAMPLE; streamFormat.audioFormat=1; streamFormat.valid=true;
  Serial.printf("[TCP] Connecting to %s:%u\n",settings.phoneHost.c_str(),settings.tcpPort);
  if (!connectStreamHost(settings.tcpPort)){ stats.lastError="TCP host unavailable"; return false; }
  streamClient.setNoDelay(true); streamClient.setTimeout(1);
  if (!beginI2S(streamFormat.sampleRate,streamFormat.channels,streamFormat.bitsPerSample)){ stats.lastError="I2S setup failed"; streamClient.stop(); return false; }
  return true;
}

static bool connectHttpWav() {
  setReceiverState(RX_CONNECTING); stats.lastHttpStatus=0;
  Serial.printf("[HTTP] Connecting to http://%s:%u/\n",settings.phoneHost.c_str(),settings.httpPort);
  if (!connectStreamHost(settings.httpPort)){ stats.lastError="HTTP host unavailable"; return false; }
  streamClient.setNoDelay(true); streamClient.setTimeout(1);
  streamClient.printf("GET /stream HTTP/1.1\r\nHost: %s:%u\r\nUser-Agent: C3MusicReceiver/%s\r\nAccept: audio/wav,audio/x-wav,*/*\r\nConnection: keep-alive\r\n\r\n", settings.phoneHost.c_str(), settings.httpPort, FIRMWARE_VERSION);
  if (!parseHttpHeaders()){ streamClient.stop(); return false; }
  StreamFormat pf;
  if (!parseWavHeader(pf)){ streamClient.stop(); return false; }
  streamFormat=pf;
  if (!beginI2S(streamFormat.sampleRate,streamFormat.channels,streamFormat.bitsPerSample)){ stats.lastError="I2S setup failed"; streamClient.stop(); return false; }
  return true;
}

static void playbackTask(void*) {
  uint8_t in[I2S_WRITE_BYTES / 2];
  uint8_t out[I2S_WRITE_BYTES];
  for(;;){
    if (!i2sReady||!bufferStarted){ vTaskDelay(pdMS_TO_TICKS(10)); continue; }

    size_t want = I2S_WRITE_BYTES / 2;
    size_t n = ringRead(in, want);
    if (n==0){
      stats.underruns++;
      bufferStarted=false;
      if (receiverState==RX_STREAMING) setReceiverState(RX_BUFFERING,"Buffer low");
      vTaskDelay(pdMS_TO_TICKS(4));
      continue;
    }

    const uint8_t *writeBuf = in;
    size_t writeLen = n;
    if (streamFormat.channels==1) {
      size_t samples = n / 2;
      int16_t *src = reinterpret_cast<int16_t*>(in);
      int16_t *dst = reinterpret_cast<int16_t*>(out);
      for (size_t i=0;i<samples;i++) { dst[i*2]=src[i]; dst[i*2+1]=src[i]; }
      writeBuf = out;
      writeLen = samples * 4;
    }

    if (i2sMux) xSemaphoreTake(i2sMux, portMAX_DELAY);
    bool ready = i2sReady;
    size_t w=0;
    esp_err_t r = ready ? i2s_write(I2S_NUM_0,writeBuf,writeLen,&w,pdMS_TO_TICKS(80)) : ESP_FAIL;
    if (i2sMux) xSemaphoreGive(i2sMux);

    if (r==ESP_OK && w>0) stats.bytesPlayed += (streamFormat.channels==1) ? (w/2) : w;
    else vTaskDelay(pdMS_TO_TICKS(2));
  }
}

static void runConnectedStream() {
  uint8_t in[NETWORK_READ_BYTES];
  stats.sessionStartedMs=millis(); stats.lastReceiveMs=millis(); ringClear(); setReceiverState(RX_BUFFERING);
  while (!stopRequested && streamClient.connected() && WiFi.status()==WL_CONNECTED) {
    int av=streamClient.available();
    if (av>0){ size_t w=min((size_t)av,sizeof(in)); int n=streamClient.read(in,w); if (n>0){ stats.bytesReceived+=n; stats.lastReceiveMs=millis(); if (!ringWrite(in,n)) vTaskDelay(pdMS_TO_TICKS(3)); if (!bufferStarted && ringSize()>=targetPrebufferBytes()){ bufferStarted=true; setReceiverState(RX_STREAMING); } } }
    else {
      if (!streamClient.connected()) {
        stats.lastError="Stream disconnected";
        break;
      }
      if (millis()-stats.lastReceiveMs>STREAM_READ_TIMEOUT_MS) {
        stats.lastError="Stream idle";
        setReceiverState(RX_BUFFERING,"Waiting for audio");
        stats.lastReceiveMs=millis();
      }
      vTaskDelay(pdMS_TO_TICKS(1));
    }
  }
}

static void streamTask(void*) {
  for(;;){
    if (WiFi.status()!=WL_CONNECTED){
      stopStreamClient();
      endI2S();
      ringClear();
      if (settings.streamEnabled && !stopRequested && receiverState!=RX_WIFI_OFFLINE && receiverState!=RX_WIFI_CONNECTING && receiverState!=RX_UPDATING){
        Serial.printf("[WIFI] Stream task sees disconnected (status=%d)\n",(int)WiFi.status());
        setReceiverState(RX_WIFI_OFFLINE,"WiFi disconnected");
      }
      vTaskDelay(pdMS_TO_TICKS(500));
      continue;
    }
    if (!settings.streamEnabled||stopRequested){ streamClient.stop(); endI2S(); ringClear(); if (receiverState!=RX_STOPPED) setReceiverState(RX_STOPPED); vTaskDelay(pdMS_TO_TICKS(150)); continue; }
    if (millis()-lastStreamAttemptMs<STREAM_RETRY_MS){ vTaskDelay(pdMS_TO_TICKS(20)); continue; }
    lastStreamAttemptMs=millis();
    bool ok=false; StreamMode tr=settings.preferredMode;
    if (tr==STREAM_MODE_TCP) ok=connectRawTcp(); else ok=connectHttpWav();
    if (!ok && settings.autoFallback && !stopRequested){
      stopStreamClient(); endI2S(); ringClear();
      StreamMode fb=(tr==STREAM_MODE_TCP)?STREAM_MODE_HTTP:STREAM_MODE_TCP;
      Serial.printf("[STREAM] Primary failed; trying %s\n",streamModeName(fb));
      if (fb==STREAM_MODE_TCP) ok=connectRawTcp(); else ok=connectHttpWav();
    }
    if (ok){ stats.reconnects++; runConnectedStream(); }
    else { stats.streamErrors++; setReceiverState(RX_ERROR,stats.lastError.length()?stats.lastError:"Stream unavailable"); }
    streamClient.stop(); endI2S(); ringClear();
    if (!stopRequested && settings.streamEnabled && settings.autoReconnect) {
      vTaskDelay(pdMS_TO_TICKS(STREAM_RETRY_MS));
    } else {
      if (!stopRequested && settings.streamEnabled && !settings.autoReconnect) {
        setReceiverState(RX_ERROR, stats.lastError.length() ? stats.lastError : "Auto reconnect disabled");
      }
      vTaskDelay(pdMS_TO_TICKS(250));
    }
  }
}

static void startStreaming(){
  stopRequested=false; settings.streamEnabled=true; lastStreamAttemptMs=0; saveSettings(); setReceiverState(RX_IDLE);
}
static void stopStreaming(){
  stopRequested=true; settings.streamEnabled=false; saveSettings(); stopStreamClient(); ringClear(); endI2S(); setReceiverState(RX_STOPPED);
}
static void reconnectStreaming(){
  stopRequested=true; stopStreamClient(); ringClear(); endI2S(); delay(100);
  stopRequested=false; settings.streamEnabled=true; lastStreamAttemptMs=0; saveSettings(); setReceiverState(RX_IDLE);
}

static String jsonEscape(const String &in){ String o; o.reserve(in.length()+8); for(size_t i=0;i<in.length();++i){ char c=in[i]; switch(c){ case'\\':o+="\\\\";break; case'"':o+="\\\"";break; case'\n':o+="\\n";break; case'\r':o+="\\r";break; case'\t':o+="\\t";break; default: if((uint8_t)c>=0x20)o+=c; } } return o; }

static String makeStatusJson(){
  String ip=(WiFi.status()==WL_CONNECTED)?WiFi.localIP().toString():"";
  String ssid=(WiFi.status()==WL_CONNECTED)?WiFi.SSID():"";
  int rssi=(WiFi.status()==WL_CONNECTED)?WiFi.RSSI():0;
  uint32_t sess=0; if (stats.sessionStartedMs>0 && (receiverState==RX_STREAMING||receiverState==RX_BUFFERING)) sess=(millis()-stats.sessionStartedMs)/1000UL;
  String r; r.reserve(1100);
  r+="{";
  r+="\"device\":\""+jsonEscape(DEVICE_NAME)+"\",";
  r+="\"version\":\""+String(FIRMWARE_VERSION)+"\",";
  r+="\"state\":\""+String(receiverStateName(receiverState))+"\",";
  r+="\"stateCode\":"+String((int)receiverState)+",";
  r+="\"mode\":\""+String(streamModeName(settings.preferredMode))+"\",";
  r+="\"streamEnabled\":"+String(settings.streamEnabled?"true":"false")+",";
  r+="\"wifiConnected\":"+String(WiFi.status()==WL_CONNECTED?"true":"false")+",";
  r+="\"ssid\":\""+jsonEscape(ssid)+"\",";
  r+="\"ip\":\""+jsonEscape(ip)+"\",";
  r+="\"hostname\":\""+String(MDNS_HOSTNAME)+".local\",";
  r+="\"rssi\":"+String(rssi)+",";
  r+="\"host\":\""+jsonEscape(settings.phoneHost)+"\",";
  r+="\"tcpPort\":"+String(settings.tcpPort)+",";
  r+="\"httpPort\":"+String(settings.httpPort)+",";
  r+="\"sampleRate\":"+String(streamFormat.sampleRate)+",";
  r+="\"channels\":"+String(streamFormat.channels)+",";
  r+="\"bits\":"+String(streamFormat.bitsPerSample)+",";
  r+="\"bufferBytes\":"+String((uint32_t)ringSize())+",";
  r+="\"bufferPercent\":"+String(ringPercent())+",";
  r+="\"bytesReceived\":"+String(stats.bytesReceived)+",";
  r+="\"bytesPlayed\":"+String(stats.bytesPlayed)+",";
  r+="\"underruns\":"+String(stats.underruns)+",";
  r+="\"reconnects\":"+String(stats.reconnects)+",";
  r+="\"streamErrors\":"+String(stats.streamErrors)+",";
  r+="\"httpStatus\":"+String(stats.lastHttpStatus)+",";
  r+="\"sessionSeconds\":"+String(sess)+",";
  r+="\"heap\":"+String(ESP.getFreeHeap())+",";
  r+="\"flashSize\":"+String(ESP.getFlashChipSize())+",";
  r+="\"oled\":"+String(settings.oledEnabled?"true":"false")+",";
  r+="\"lastError\":\""+jsonEscape(stats.lastError)+"\"";
  r+="}";
  return r;
}

static String makeConfigJson(){
  String r; r.reserve(500);
  r+="{";
  r+="\"host\":\""+jsonEscape(settings.phoneHost)+"\",";
  r+="\"tcpPort\":"+String(settings.tcpPort)+",";
  r+="\"httpPort\":"+String(settings.httpPort)+",";
  r+="\"mode\":\""+String(settings.preferredMode==STREAM_MODE_HTTP?"http":"tcp")+"\",";
  r+="\"autoFallback\":"+String(settings.autoFallback?"true":"false")+",";
  r+="\"autoReconnect\":"+String(settings.autoReconnect?"true":"false")+",";
  r+="\"oled\":"+String(settings.oledEnabled?"true":"false")+",";
  r+="\"streamEnabled\":"+String(settings.streamEnabled?"true":"false")+",";
  r+="\"bufferMs\":"+String(settings.targetBufferMs)+",";
  r+="\"i2s\":{\"bclk\":"+String(I2S_BCLK_PIN)+",\"lrclk\":"+String(I2S_LRCLK_PIN)+",\"dout\":"+String(I2S_DOUT_PIN)+"}";
  r+="}";
  return r;
}

static void sendJson(int code, const String &body){ server.sendHeader("Cache-Control","no-store,no-cache,must-revalidate,max-age=0"); server.sendHeader("Pragma","no-cache"); server.send(code,"application/json",body); }
static bool requirePost(){ if (server.method()!=HTTP_POST){ sendJson(405,"{\"ok\":false,\"error\":\"POST required\"}"); return false; } return true; }

static String htmlPage(){
  return F("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>C3 Music Receiver</title><style>:root{--bg:#14121f;--surface:#1e1b2e;--text:#e7e0f4;--muted:#c9c0d5;--primary:#cbb8ff;--onprimary:#2f1765;--outline:#938f9d;--ok:#87e8ae;--bad:#ffb4ab}body{margin:0;background:var(--bg);color:var(--text);font:14px system-ui,sans-serif}header{position:sticky;top:0;background:rgba(20,18,31,.95);backdrop-filter:blur(12px);border-bottom:1px solid rgba(255,255,255,.08);padding:12px 16px}h1{font-size:18px;margin:0}.chip{display:inline-block;border:1px solid var(--outline);border-radius:999px;padding:4px 8px;font-size:12px}.chip.ok{border-color:#4b9965;color:var(--ok)}.chip.bad{border-color:#a65351;color:var(--bad)}main{max-width:800px;margin:0 auto;padding:16px 16px 92px}.tab{display:none}.tab.active{display:block}.card{background:var(--surface);border:1px solid rgba(255,255,255,.08);border-radius:18px;padding:16px;margin-bottom:12px;box-shadow:0 8px 24px rgba(0,0,0,.16)}.row{display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid rgba(255,255,255,.06)}.row:last-child{border-bottom:0}.label{color:var(--muted)}.value{text-align:right;max-width:62%;overflow-wrap:anywhere}.hero{font-size:28px;font-weight:800;letter-spacing:-.02em;margin:12px 0 6px;line-height:1.15}.meter{height:8px;background:#332e3e;border-radius:999px;overflow:hidden;margin:14px 0 8px}.meter i{display:block;height:100%;width:0;background:var(--primary);border-radius:inherit;transition:width .25s ease}button{border:0;border-radius:999px;background:var(--primary);color:var(--onprimary);font-weight:700;padding:10px 14px;cursor:pointer}button.secondary{background:transparent;color:var(--primary);border:1px solid #8c78b7}button.danger{background:#ffb4ab;color:#690005}.buttons{display:flex;flex-wrap:wrap;gap:8px;margin-top:10px}.field{margin:10px 0}.field label{display:block;color:var(--muted);font-size:12px;margin:0 0 4px 4px}input,select{width:100%;border:1px solid var(--outline);background:#272334;color:var(--text);border-radius:10px;padding:10px;font:inherit}.switchrow{display:flex;justify-content:space-between;align-items:center;padding:8px 0}.switch{appearance:none;width:44px;height:24px;border-radius:20px;background:#5c5666;position:relative;border:0}.switch:checked{background:#cbb8ff}.switch:before{content:\"\";position:absolute;width:18px;height:18px;top:3px;left:3px;background:#fff;border-radius:50%;transition:.15s}.switch:checked:before{left:21px;background:#34205f}nav{position:fixed;z-index:20;bottom:0;left:0;right:0;display:flex;justify-content:center;gap:6px;padding:8px max(8px,env(safe-area-inset-left)) calc(8px + env(safe-area-inset-bottom));background:rgba(30,27,46,.96);backdrop-filter:blur(12px);border-top:1px solid rgba(255,255,255,.09)}nav button{min-width:90px;background:transparent;color:var(--muted)}nav button.active{background:#4c3e6d;color:#f0e7ff}.small{font-size:12px;color:var(--muted)}.hidden{display:none!important}h2{font-size:18px;margin:0 0 10px}h3{font-size:15px;margin:0 0 8px}button{min-height:40px}button:disabled{opacity:.55;cursor:not-allowed}@media(max-width:520px){main{padding-left:12px;padding-right:12px}.card{padding:14px}.buttons button{flex:1 1 auto}.value{max-width:58%}nav button{min-width:0;flex:1}.hero{font-size:25px}}.mono{font-family:ui-monospace,Consolas,monospace;font-size:12px}#toast{position:fixed;left:50%;bottom:70px;transform:translate(-50%,20px);opacity:0;background:#ece6f5;color:#201b29;border-radius:10px;padding:10px 14px;transition:.2s}#toast.show{opacity:1;transform:translate(-50%,0)}progress{width:100%;height:10px;accent-color:#cbb8ff}</style></head><body><header><h1>C3 Music Receiver <span class=\"chip\" id=\"stateChip\">Loading</span></h1><div class=\"small\" id=\"address\">c3music.local</div></header><main><section class=\"tab active\" id=\"now\"><div class=\"card\"><h2>Now Playing</h2><div class=\"hero\" id=\"state\">Connecting…</div><div class=\"small\" id=\"format\">Waiting for status</div><div class=\"meter\"><i id=\"bufferBar\" style=\"width:0%\"></i></div><div class=\"small\" id=\"bufferText\">Buffer: —</div><div class=\"buttons\"><button onclick=\"act('/api/stream/start')\">Start</button><button class=\"secondary\" onclick=\"act('/api/stream/stop')\">Stop</button><button class=\"secondary\" onclick=\"act('/api/stream/reconnect')\">Reconnect</button></div></div><div class=\"card\"><h3>Source</h3><div class=\"row\"><span class=\"label\">Mode</span><span class=\"value\" id=\"mode\">—</span></div><div class=\"row\"><span class=\"label\">Host</span><span class=\"value\" id=\"host\">—</span></div><div class=\"row\"><span class=\"label\">Session</span><span class=\"value\" id=\"session\">—</span></div></div><div class=\"card\"><h3>Audio health</h3><div class=\"row\"><span class=\"label\">Underruns</span><span class=\"value\" id=\"underruns\">0</span></div><div class=\"row\"><span class=\"label\">Reconnects</span><span class=\"value\" id=\"reconnects\">0</span></div><div class=\"row\"><span class=\"label\">Last error</span><span class=\"value\" id=\"lastError\">None</span></div></div></section><section class=\"tab\" id=\"wifi\"><div class=\"card\"><h2>Wi‑Fi</h2><div class=\"row\"><span class=\"label\">Status</span><span class=\"value\" id=\"wifiStatus\">—</span></div><div class=\"row\"><span class=\"label\">SSID</span><span class=\"value\" id=\"ssid\">—</span></div><div class=\"row\"><span class=\"label\">IP</span><span class=\"value\" id=\"ip\">—</span></div><div class=\"row\"><span class=\"label\">Hostname</span><span class=\"value\">c3music.local</span></div><div class=\"row\"><span class=\"label\">Signal</span><span class=\"value\" id=\"rssi\">—</span></div><div class=\"buttons\"><button onclick=\"act('/api/wifi/reconnect')\">Reconnect Wi‑Fi</button></div></div></section><section class=\"tab\" id=\"settings\"><div class=\"card\"><h2>Stream settings</h2><div class=\"field\"><label>Phone host / IP</label><input id=\"hostInput\" autocomplete=\"off\"></div><div class=\"field\"><label>Preferred stream</label><select id=\"modeInput\"><option value=\"tcp\">Raw TCP PCM — port 50005</option><option value=\"http\">HTTP WAV/PCM — port 8080</option></select></div><div class=\"field\"><label>TCP port</label><input id=\"tcpInput\" type=\"number\" min=\"1\" max=\"65535\"></div><div class=\"field\"><label>HTTP port</label><input id=\"httpInput\" type=\"number\" min=\"1\" max=\"65535\"></div><div class=\"field\"><label>Target buffer (ms)</label><input id=\"bufferInput\" type=\"number\" min=\"80\" max=\"700\" step=\"10\"></div><div class=\"switchrow\"><span>Automatic TCP/HTTP fallback</span><input class=\"switch\" id=\"fallbackInput\" type=\"checkbox\"></div><div class=\"switchrow\"><span>Automatic stream reconnect</span><input class=\"switch\" id=\"autoreconnectInput\" type=\"checkbox\"></div><div class=\"buttons\"><button onclick=\"saveCfg()\">Save and reconnect</button></div></div><div class=\"card\"><h2>Device settings</h2><div class=\"switchrow\"><span>OLED enabled</span><input class=\"switch\" id=\"oledInput\" type=\"checkbox\" onchange=\"saveCfg()\"></div><div class=\"row\"><span class=\"label\">Firmware</span><span class=\"value\" id=\"version\">—</span></div><div class=\"row\"><span class=\"label\">Free heap</span><span class=\"value\" id=\"heap\">—</span></div></div><div class=\"card\"><h2>Firmware OTA</h2><p class=\"small\">Select a .bin file. Streaming will stop during update.</p><input id=\"firmware\" type=\"file\" accept=\".bin\"><div class=\"buttons\"><button id=\"otaBtn\" onclick=\"uploadFw()\">Install firmware</button></div><progress id=\"otaProg\" class=\"hidden\" value=\"0\" max=\"100\"></progress><div class=\"small\" id=\"otaTxt\"></div></div><div class=\"card\"><h2>Maintenance</h2><div class=\"buttons\"><button class=\"secondary\" onclick=\"act('/api/system/clear-stats')\">Clear stats</button><button class=\"secondary\" onclick=\"act('/api/system/reboot')\">Restart</button><button class=\"danger\" onclick=\"if(confirm('Factory reset?'))act('/api/system/factory-reset')\">Factory reset</button></div></div></section></main><nav><button class=\"active\" data-tab=\"now\">Now</button><button data-tab=\"wifi\">Wi‑Fi</button><button data-tab=\"settings\">Settings</button></nav><div id=\"toast\"></div><script>const $=id=>document.getElementById(id);let cfgLoaded=false;function toast(t){const x=$('toast');x.textContent=t;x.classList.add('show');setTimeout(()=>x.classList.remove('show'),2500)}function text(id,v){const e=$(id);if(e)e.textContent=v}function time(s){s=Number(s||0);const h=Math.floor(s/3600),m=Math.floor(s%3600/60),q=s%60;return[h,m,q].map((x,i)=>String(x).padStart(2,'0')).join(':')}function apply(d){text('state',d.state||'—');text('format',`${d.sampleRate||0} Hz · ${d.bits||0}-bit · ${d.channels===2?'Stereo':'Mono'}`);text('mode',d.mode||'—');text('host',`${d.host||'—'} · ${d.mode==='HTTP WAV'?d.httpPort:d.tcpPort}`);text('session',time(d.sessionSeconds));text('underruns',d.underruns||0);text('reconnects',d.reconnects||0);text('lastError',d.lastError||'None');text('wifiStatus',d.wifiConnected?'Connected':'Disconnected');text('ssid',d.ssid||'—');text('ip',d.ip||'—');text('rssi',d.wifiConnected?`${d.rssi} dBm`:'—');text('address',d.ip?`http://${d.hostname||'c3music.local'} · ${d.ip}`:'c3music.local');text('version',d.version||'—');text('heap',d.heap?`${Math.round(d.heap/1024)} KB`:'—');const p=Number(d.bufferPercent||0);$('bufferBar').style.width=p+'%';text('bufferText',`Buffer: ${p}% · ${d.bufferBytes||0} bytes`);const c=$('stateChip');c.textContent=d.state||'Unknown';c.className='chip '+(d.state==='Streaming'?'ok':d.state==='Error'||d.state==='WiFi offline'?'bad':'')}async function poll(){try{const r=await fetch('/api/status',{cache:'no-store'});if(!r.ok)throw 0;apply(await r.json());if(!cfgLoaded)loadCfg()}catch(e){text('state','Web lost');$('stateChip').textContent='Offline';$('stateChip').className='chip bad'}}async function loadCfg(){try{const d=await (await fetch('/api/config',{cache:'no-store'})).json();$('hostInput').value=d.host||'';$('tcpInput').value=d.tcpPort||50005;$('httpInput').value=d.httpPort||8080;$('modeInput').value=d.mode||'tcp';$('bufferInput').value=d.bufferMs||250;$('fallbackInput').checked=!!d.autoFallback;$('autoreconnectInput').checked=!!d.autoReconnect;$('oledInput').checked=!!d.oled;cfgLoaded=true}catch(e){}}async function act(url){try{const r=await fetch(url,{method:'POST'});const d=await r.json();toast(d.ok?'Done':(d.error||'Failed'));setTimeout(poll,300)}catch(e){toast('Request failed')}}async function saveCfg(){const body=new URLSearchParams({host:$('hostInput').value.trim(),tcpPort:$('tcpInput').value,httpPort:$('httpInput').value,bufferMs:$('bufferInput').value,mode:$('modeInput').value,autoFallback:$('fallbackInput').checked?'1':'0',autoReconnect:$('autoreconnectInput').checked?'1':'0',oled:$('oledInput').checked?'1':'0'});try{const r=await fetch('/api/config',{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded'},body});const d=await r.json();toast(d.ok?'Saved':'Save failed');cfgLoaded=false;setTimeout(poll,500)}catch(e){toast('Save failed')}}function uploadFw(){const f=$('firmware').files[0];if(!f){toast('Choose .bin first');return}if(!confirm(`Install ${f.name}?`))return;const xhr=new XMLHttpRequest(),form=new FormData();form.append('firmware',f);$('otaProg').classList.remove('hidden');$('otaProg').value=0;$('otaTxt').textContent='Uploading…';$('otaBtn').disabled=true;xhr.upload.onprogress=e=>{if(e.lengthComputable){const p=Math.round(e.loaded/e.total*100);$('otaProg').value=p;$('otaTxt').textContent=`Uploading ${p}%`}};xhr.onload=()=>{$('otaBtn').disabled=false;if(xhr.status===200){$('otaProg').value=100;$('otaTxt').textContent='Update accepted. Restarting…';toast('Firmware update successful')}else{$('otaTxt').textContent='Update failed: '+xhr.responseText;toast('OTA failed')}};xhr.onerror=()=>{$('otaBtn').disabled=false;$('otaTxt').textContent='Upload failed';toast('OTA failed')};xhr.open('POST','/api/ota');xhr.send(form)}document.querySelectorAll('nav button').forEach(b=>b.onclick=()=>{document.querySelectorAll('nav button').forEach(x=>x.classList.remove('active'));document.querySelectorAll('.tab').forEach(x=>x.classList.remove('active'));b.classList.add('active');const panel=$(b.dataset.tab);if(panel)panel.classList.add('active');window.scrollTo({top:0,behavior:'smooth'})});poll();setInterval(poll,1000);</script></body></html>");
}

static bool otaUploadFailed=false; static String otaUploadError;
static void handleFirmwareUpload(){
  HTTPUpload &up=server.upload();
  if (up.status==UPLOAD_FILE_START){ otaUploadFailed=false; otaUploadError=""; stopRequested=true; stopStreamClient(); ringClear(); endI2S(); setReceiverState(RX_UPDATING); size_t sz=UPDATE_SIZE_UNKNOWN; if (!Update.begin(sz,U_FLASH)){ otaUploadFailed=true; otaUploadError=Update.errorString(); Serial.printf("[OTA] Begin failed: %s\n",otaUploadError.c_str()); } else Serial.printf("[OTA] Start: %s\n",up.filename.c_str()); }
  else if (up.status==UPLOAD_FILE_WRITE){ if (!otaUploadFailed){ size_t w=Update.write(up.buf,up.currentSize); if (w!=up.currentSize){ otaUploadFailed=true; otaUploadError=Update.errorString(); Serial.printf("[OTA] Write failed: %s\n",otaUploadError.c_str()); } } }
  else if (up.status==UPLOAD_FILE_END){ if (!otaUploadFailed){ if (!Update.end(true)){ otaUploadFailed=true; otaUploadError=Update.errorString(); Serial.printf("[OTA] End failed: %s\n",otaUploadError.c_str()); } else Serial.printf("[OTA] Success: %u bytes\n",up.totalSize); } }
  else if (up.status==UPLOAD_FILE_ABORTED){ otaUploadFailed=true; otaUploadError="Upload aborted"; Update.abort(); }
}

static void setupWebServer(){
  server.on("/",HTTP_GET,[]{ server.sendHeader("Cache-Control","no-store,no-cache,must-revalidate,max-age=0"); server.send(200,"text/html; charset=utf-8",htmlPage()); });
  server.on("/api/status",HTTP_GET,[]{ sendJson(200,makeStatusJson()); });
  server.on("/api/config",HTTP_GET,[]{ sendJson(200,makeConfigJson()); });
  server.on("/api/config",HTTP_POST,[]{
    if (!requirePost()) return;
    String host=server.arg("host"); host.trim();
    if (host.length()==0||host.length()>63){ sendJson(400,"{\"ok\":false,\"error\":\"Invalid host\"}"); return; }
    uint32_t tp=server.arg("tcpPort").toInt(), hp=server.arg("httpPort").toInt();
    if (tp<1||tp>65535||hp<1||hp>65535){ sendJson(400,"{\"ok\":false,\"error\":\"Invalid port\"}"); return; }
    settings.phoneHost=host; settings.tcpPort=(uint16_t)tp; settings.httpPort=(uint16_t)hp;
    settings.preferredMode=(server.arg("mode")=="http")?STREAM_MODE_HTTP:STREAM_MODE_TCP;
    uint32_t bm=server.arg("bufferMs").toInt(); if (bm<80||bm>700){ sendJson(400,"{\"ok\":false,\"error\":\"Invalid buffer\"}"); return; }
    settings.autoFallback=(server.arg("autoFallback")=="1"); settings.autoReconnect=(server.arg("autoReconnect")=="1"); settings.oledEnabled=(server.arg("oled")=="1"); settings.targetBufferMs=(uint16_t)bm;
    if (oledAvailable) oled.setPowerSave(settings.oledEnabled?0:1);
    saveSettings(); reconnectStreaming(); sendJson(200,"{\"ok\":true}");
  });
  server.on("/api/stream/start",HTTP_POST,[]{ if(!requirePost())return; startStreaming(); sendJson(200,"{\"ok\":true}"); });
  server.on("/api/stream/stop",HTTP_POST,[]{ if(!requirePost())return; stopStreaming(); sendJson(200,"{\"ok\":true}"); });
  server.on("/api/stream/reconnect",HTTP_POST,[]{ if(!requirePost())return; reconnectStreaming(); sendJson(200,"{\"ok\":true}"); });
  server.on("/api/wifi/reconnect",HTTP_POST,[]{ if(!requirePost())return; WiFi.disconnect(false,false); stopStreamClient(); mdnsStarted=false; lastWifiAttemptMs=0; connectWifiIfNeeded(); sendJson(200,"{\"ok\":true}"); });
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
  Serial.println("Raw PCM Wi-Fi receiver — TCP primary / HTTP WAV fallback");
  Serial.println("============================================================");
  loadSettings();
  audioDataSemaphore=xSemaphoreCreateBinary();
  i2sMux=xSemaphoreCreateMutex();
  streamClientMux=xSemaphoreCreateMutex();
  displayBegin(); setReceiverState(RX_BOOTING);
  WiFi.persistent(false); WiFi.mode(WIFI_STA); WiFi.setSleep(false); WiFi.setAutoReconnect(true);
  setupWebServer();
  BaseType_t ok1=xTaskCreate(playbackTask,"i2sPlayback",4096,nullptr,3,&playbackTaskHandle);
  BaseType_t ok2=xTaskCreate(streamTask,"pcmStream",6144,nullptr,2,&streamTaskHandle);
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
    if (receiverState==RX_STREAMING&&ringSize()<(target/8)) setReceiverState(RX_BUFFERING,"Buffer low");
    else if (receiverState==RX_BUFFERING&&bufferStarted&&ringSize()>=(target/2)) setReceiverState(RX_STREAMING);
  }
  delay(2);
}
