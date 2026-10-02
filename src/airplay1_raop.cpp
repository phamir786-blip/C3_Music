#include "airplay1_raop.h"

#include <WiFi.h>
#include <ESPmDNS.h>
#include <esp_mac.h>
#include <mdns.h>

namespace {
WiFiServer raopServer(7000);
WiFiClient raopClient;
bool running = false;
uint16_t listenPort = 7000;
String instanceName;

void sendRtspResponse(const String &request) {
  int cseq = 0;
  int p = request.indexOf("CSeq:");
  if (p >= 0) {
    p += 5;
    while (p < (int)request.length() && request[p] == ' ') ++p;
    cseq = request.substring(p).toInt();
  }

  String method = request;
  int sp = method.indexOf(' ');
  if (sp > 0) method = method.substring(0, sp);

  String response;
  response.reserve(512);
  response += "RTSP/1.0 200 OK\r\n";
  response += "CSeq: " + String(cseq) + "\r\n";
  if (method == "OPTIONS") {
    response += "Public: OPTIONS, ANNOUNCE, SETUP, RECORD, PAUSE, FLUSH, TEARDOWN, GET_PARAMETER, SET_PARAMETER\r\n";
  }
  response += "Server: C3 Music AirPlay1 Experimental\r\n";
  response += "\r\n";
  raopClient.print(response);
}

void advertise() {
  if (WiFi.status() != WL_CONNECTED) return;

  uint8_t mac[6] = {};
  esp_read_mac(mac, ESP_MAC_WIFI_STA);

  char instance[64];
  snprintf(instance, sizeof(instance),
           "%02X%02X%02X%02X%02X%02X@C3 Music",
           mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
  instanceName = instance;

  // AirPlay 1 / RAOP discovery record. Audio protocol implementation is
  // deliberately isolated in this experimental module.
  mdns_txt_item_t txt[] = {
    {(char*)"txtvers", (char*)"1"},
    {(char*)"ch",      (char*)"2"},
    {(char*)"cn",      (char*)"0,1"},
    {(char*)"et",      (char*)"0,1"},
    {(char*)"md",      (char*)"0,1,2"},
    {(char*)"pw",      (char*)"false"},
    {(char*)"sr",      (char*)"44100"},
    {(char*)"ss",      (char*)"16"},
    {(char*)"tp",      (char*)"UDP"},
    {(char*)"vn",      (char*)"65537"},
    {(char*)"vs",      (char*)"130.14"}
  };

  if (mdns_service_add(NULL, "_raop", "_tcp", listenPort, txt, sizeof(txt) / sizeof(txt[0]))) {
    Serial.println("[AIRPLAY1] mDNS _raop service add failed");
    return;
  }

  if (mdns_service_instance_name_set("_raop", "_tcp", instanceName.c_str())) {
    Serial.println("[AIRPLAY1] mDNS instance-name setup failed");
  }

  Serial.printf("[AIRPLAY1] Advertising %s on _raop._tcp:%u\n",
                instanceName.c_str(), listenPort);
}

void unadvertise() {
  if (!running) return;
  mdns_service_remove("_raop", "_tcp");
  Serial.println("[AIRPLAY1] RAOP advertisement stopped");
}
}

bool airplay1Start(uint16_t port) {
  if (running) return true;
  if (WiFi.status() != WL_CONNECTED) return false;

  listenPort = port;
  raopServer = WiFiServer(listenPort);
  raopServer.begin();
  raopServer.setNoDelay(true);

  advertise();
  running = true;
  Serial.printf("[AIRPLAY1] Experimental receiver started. Free heap: %u\n",
                ESP.getFreeHeap());
  return true;
}

void airplay1Loop() {
  if (!running) return;

  if (!raopClient || !raopClient.connected()) {
    if (raopClient) raopClient.stop();
    WiFiClient candidate = raopServer.accept();
    if (candidate) {
      raopClient = candidate;
      raopClient.setTimeout(250);
      Serial.printf("[AIRPLAY1] RTSP client connected from %s:%u\n",
                    raopClient.remoteIP().toString().c_str(),
                    raopClient.remotePort());
    }
    return;
  }

  while (raopClient.available()) {
    String request = raopClient.readStringUntil('\n');
    request += "\n";

    // Consume the remainder of the RTSP headers.
    String headers = request;
    uint32_t deadline = millis() + 500;
    while (millis() < deadline) {
      if (!raopClient.available()) {
        delay(1);
        continue;
      }
      String line = raopClient.readStringUntil('\n');
      headers += line;
      if (line == "\r\n" || line == "\n") break;
    }

    Serial.printf("[AIRPLAY1] RTSP request: %s", request.c_str());
    sendRtspResponse(headers);
  }
}

void airplay1Stop() {
  if (!running) return;
  if (raopClient) raopClient.stop();
  raopServer.stop();
  unadvertise();
  running = false;
  Serial.println("[AIRPLAY1] Experimental receiver stopped");
}

bool airplay1IsRunning() {
  return running;
}
