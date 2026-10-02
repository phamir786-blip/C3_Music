#include "airplay1_raop.h"
#include "airplay1_alac.h"

#include <WiFi.h>
#include <WiFiUdp.h>
#include <ESPmDNS.h>
#include <esp_mac.h>
#include <mbedtls/pk.h>
#include <mbedtls/rsa.h>
#include <mbedtls/aes.h>
#include <mbedtls/base64.h>
#include <mbedtls/ctr_drbg.h>
#include <mbedtls/entropy.h>
#include <string.h>

namespace {
WiFiServer rtspServer(7000);
WiFiClient rtspClient;
WiFiUDP audioUdp, controlUdp, timingUdp;
bool running=false, recording=false, haveAes=false;
uint16_t rtspPort=7000, audioPort=6000, controlPort=6001, timingPort=6002;
uint16_t remoteControlPort=0, remoteTimingPort=0;
uint8_t audioPayloadType=96;
uint32_t recordRtpTime=0;
IPAddress remoteIp;
uint16_t lastSeq=0;
uint32_t lastRtptime=0;
Airplay1PcmSink pcmSink=nullptr;

alac_file* alac=nullptr;
mbedtls_aes_context aesDec;
uint8_t aesKey[16]={}, aesIv[16]={};
mbedtls_pk_context rsaKey;
mbedtls_entropy_context entropy;
mbedtls_ctr_drbg_context ctr;
bool cryptoReady=false;

static const char AIRPORT_RSA_KEY[] = R"KEY(
-----BEGIN RSA PRIVATE KEY-----
MIIEpQIBAAKCAQEA59dE8qLieItsH1WgjrcFRKj6eUWqi+bGLOX1HL3U3GhC/j0Qg90u3sG/1CUt
wC5vOYvfDmFI6oSFXi5ELabWJmT2dKHzBJKa3k9ok+8t9ucRqMd6DZHJ2YCCLlDRKSKv6kDqnw4U
wPdpOMXziC/AMj3Z/lUVX1G7WSHCAWKf1zNS1eLvqr+boEjXuBOitnZ/bDzPHrTOZz0Dew0uowxf
/+sG+NCK3eQJVxqcaJ/vEHKIVd2M+5qL71yJQ+87X6oV3eaYvt3zWZYD6z5vYTcrtij2VZ9Zmni/
UAaHqn9JdsBWLUEpVviYnhimNVvYFZeCXg/IdTQ+x4IRdiXNv5hEewIDAQABAoIBAQDl8Axy9XfW
BLmkzkEiqoSwF0PsmVrPzH9KsnwLGH+QZlvjWd8SWYGN7u1507HvhF5N3drJoVU3O14nDY4TFQAa
LlJ9VM35AApXaLyY1ERrN7u9ALKd2LUwYhM7Km539O4yUFYikE2nIPscEsA5ltpxOgUGCY7b7ez5
NtD6nL1ZKauw7aNXmVAvmJTcuPxWmoktF3gDJKK2wxZuNGcJE0uFQEG4Z3BrWP7yoNuSK3dii2jm
lpPHr0O/KnPQtzI3eguhe0TwUem/eYSdyzMyVx/YpwkzwtYL3s5rKQLtvLzfAqdBxBurciz
aaA/L0HIgAmOit1GJA2saMxTVPNhAoGBAPfgv1oeZxgxmotiCcMXFEQEWflzhWYTsXrhUIuz5jFu
a39GLS99ZEErhLdrwj8rDDViRVJ5skOp9zFvlYAHs0xh92ji1E7V/ysnKBfsMrPkk5KSKPrnjndM
oPdevWnVkgJ5jxFuNgxkOLMuG9i53B4yMvDTCRiIPMQ++N2iLDaRAoGBAO9v//mU8eVkQaoANf0Z
oMjW8CN4xwWA2cSEIHkd9AfFkftuv8oyLDCG3ZAf0vrhrrtkrfa7ef+AUb69DNggq4mHQAYBp7L+
k5DKzJrKuO0r+R0YbY9pZD1+/g9dVt91d6LQNepUE/yY2PP5CNoFmjedpLHMOPFdVgqDzDFxU8hL
AoGBANDrr7xAJbqBjHVwIzQ4To9pb4BNeqDndk5Qe7fT3+/H1njGaC0/rXE0Qb7q5ySgnsCb3DvA
cJyRM9SJ7OKlGt0FMSdJD5KG0XPIpAVNwgpXXH5MDJg09KHeh0kXo+QA6viFBi21y340NonnEfdf
54PX4ZGS/Xac1UK+pLkBB+zRAoGAf0AY3H3qKS2lMEI4bzEFoHeK3G895pDaK3TFBVmD7fV0Zhov
17fegFPMwOII8MisYm9ZfT2Z0s5Ro3s5rkt+nvLAdfC/PYPKzTLalpGSwomSNYJcB9HNMlmhkGzc
1JnLYT4iyUyx6pcZBmCd8bD0iwY/FzcgNDaUmbX9+XDvRA0CgYEAkE7pIPlE71qvfJQgoA9em0gI
LAuE4Pu13aKiJnfft7hIjbK+5kyb3TysZvoyDnb3HOKvInK7vXbKuU4ISgxB2bB3HcYzQMGsz1qJ
2gG0N5hvJpzwwhbhXqFKA4zaaSrw622wDniAK5MlIE0tIAKKP4yxNGjoD2QYjhBGuhvkWKY=
-----END RSA PRIVATE KEY-----
)KEY";

String headerValue(const String& req,const char* name){
  String key=String(name)+":";
  int p=req.indexOf(key);
  if(p<0){ key=String(name)+" :"; p=req.indexOf(key); }
  if(p<0) return "";
  p+=key.length(); while(p<(int)req.length()&&(req[p]==' '||req[p]=='\t'))p++;
  int e=req.indexOf("\r\n",p); if(e<0)e=req.indexOf('\n',p);
  if(e<0)e=req.length(); return req.substring(p,e);
}
int cseqOf(const String& req){return headerValue(req,"CSeq").toInt();}

int b64(const String& s,uint8_t* out,size_t cap){
  size_t olen=0;
  String p=s; while(p.length()%4)p+="=";
  if(mbedtls_base64_decode(out,cap,&olen,(const uint8_t*)p.c_str(),p.length())!=0)return -1;
  return (int)olen;
}
String b64enc(const uint8_t* in,size_t len,bool trim=true){
  size_t n=0; size_t cap=((len+2)/3)*4+4; uint8_t* out=(uint8_t*)malloc(cap);
  if(!out)return "";
  if(mbedtls_base64_encode(out,cap,&n,in,len)!=0){free(out);return "";}
  String s=(const char*)out; free(out);
  if(trim)while(s.endsWith("="))s.remove(s.length()-1);
  return s;
}

bool rsaInit(){
  if(cryptoReady)return true;
  mbedtls_pk_init(&rsaKey); mbedtls_entropy_init(&entropy); mbedtls_ctr_drbg_init(&ctr);
  const char pers[]="C3Music-AirPlay1";
  if(mbedtls_ctr_drbg_seed(&ctr,mbedtls_entropy_func,&entropy,(const unsigned char*)pers,sizeof(pers)-1)!=0)return false;
  int r=mbedtls_pk_parse_key(&rsaKey,(const unsigned char*)AIRPORT_RSA_KEY,strlen(AIRPORT_RSA_KEY)+1,nullptr,0);
  if(r!=0){mbedtls_pk_free(&rsaKey);return false;}
  cryptoReady=true; return true;
}

String appleResponse(const String& challenge){
  if(!rsaInit())return "";
  uint8_t challengeBytes[32]={};
  int n=b64(challenge,challengeBytes,sizeof(challengeBytes));
  if(n<0)n=0;
  if(n>16)n=16;

  uint8_t data[32]={};
  uint8_t* p=data;
  memcpy(p,challengeBytes,n); p+=n;

  IPAddress ip=WiFi.localIP();
  uint8_t ipBytes[4]={ip[0],ip[1],ip[2],ip[3]};
  memcpy(p,ipBytes,4); p+=4;

  uint8_t mac[6];
  esp_read_mac(mac,ESP_MAC_WIFI_STA);
  memcpy(p,mac,6); p+=6;

  // Apple Challenge is a raw 32-byte message signed with the
  // AirPort private key using PKCS#1 v1.5 "private encrypt".
  memset(p,0,sizeof(data)-(size_t)(p-data));

  mbedtls_rsa_context* rsa=mbedtls_pk_rsa(rsaKey);
  mbedtls_rsa_set_padding(rsa,MBEDTLS_RSA_PKCS_V15,MBEDTLS_MD_NONE);

  size_t keyLen=mbedtls_rsa_get_len(rsa);
  if(keyLen==0||keyLen>256)return "";

  uint8_t* enc=(uint8_t*)malloc(keyLen);
  if(!enc)return "";

  int r=mbedtls_rsa_pkcs1_encrypt(
    rsa,nullptr,nullptr,MBEDTLS_RSA_PRIVATE,
    sizeof(data),data,enc);

  if(r!=0){ free(enc); return ""; }

  String response=b64enc(enc,keyLen,true);
  free(enc);
  return response;
}

bool parseFmtp(const String& body){
  int p=body.indexOf("a=fmtp:");
  if(p<0)return false; p=body.indexOf(' ',p); if(p<0)p=body.indexOf('\t',p); if(p<0)return false;
  int e=body.indexOf("\r\n",p); if(e<0)e=body.indexOf('\n',p); if(e<0)e=body.length();
  String f=body.substring(p+1,e); f.trim();
  int vals[11]={}; int count=0; char buf[160]; f.toCharArray(buf,sizeof(buf)); char* tok=strtok(buf," ,\t");
  while(tok&&count<11){vals[count++]=atoi(tok);tok=strtok(nullptr," ,\t");}
  if(count<11)return false;
  const uint32_t sampleRate=(uint32_t)vals[10];
  if(vals[0]<=0||vals[2]!=16||vals[6]!=2||sampleRate==0)return false;
  if(alac)alac_free(alac);
  alac=alac_create(16,2);
  if(!alac)return false;
  // The ALAC decoder expects the standard 48-byte magic cookie:
  // size/frma/alac + size/alac + version/flags + the 11 fmtp values.
  // Keep the 4-byte version/flags field at offset 20; the previous 44-byte
  // cookie placed max_samples_per_frame at offset 20, shifting every field
  // by four bytes and leaving the decoder with an invalid frame size.
  uint8_t cookie[48]={0};
  cookie[0]=0;cookie[1]=0;cookie[2]=0;cookie[3]=48;
  memcpy(cookie+4,"frma",4); memcpy(cookie+8,"alac",4);
  cookie[12]=0;cookie[13]=0;cookie[14]=0;cookie[15]=36; memcpy(cookie+16,"alac",4);
  cookie[24]=(uint8_t)(vals[0]>>24); cookie[25]=(uint8_t)(vals[0]>>16); cookie[26]=(uint8_t)(vals[0]>>8); cookie[27]=(uint8_t)vals[0];
  cookie[28]=(uint8_t)vals[1]; cookie[29]=(uint8_t)vals[2]; cookie[30]=(uint8_t)vals[3]; cookie[31]=(uint8_t)vals[4];
  cookie[32]=(uint8_t)vals[5]; cookie[33]=(uint8_t)vals[6];
  cookie[34]=(uint8_t)(vals[7]>>8); cookie[35]=(uint8_t)vals[7];
  cookie[36]=(uint8_t)(vals[8]>>24); cookie[37]=(uint8_t)(vals[8]>>16); cookie[38]=(uint8_t)(vals[8]>>8); cookie[39]=(uint8_t)vals[8];
  cookie[40]=(uint8_t)(vals[9]>>24); cookie[41]=(uint8_t)(vals[9]>>16); cookie[42]=(uint8_t)(vals[9]>>8); cookie[43]=(uint8_t)vals[9];
  cookie[44]=(uint8_t)(vals[10]>>24); cookie[45]=(uint8_t)(vals[10]>>16); cookie[46]=(uint8_t)(vals[10]>>8); cookie[47]=(uint8_t)vals[10];
  alac_set_info(alac,(char*)cookie);
  Serial.printf("[AIRPLAY1] ALAC %lu Hz, %d-bit, %dch, frame=%d\n",(unsigned long)sampleRate,vals[2],vals[6],vals[0]);
  return true;
}

bool decryptAesKey(const String& s){
  uint8_t enc[256], plain[64]; int n=b64(s,enc,sizeof(enc)); if(n<=0||!rsaInit())return false;
  mbedtls_rsa_context* rsa=mbedtls_pk_rsa(rsaKey);
  mbedtls_rsa_set_padding(rsa,MBEDTLS_RSA_PKCS_V21,MBEDTLS_MD_SHA1);
  size_t olen=0; int r=mbedtls_rsa_rsaes_oaep_decrypt(rsa,mbedtls_ctr_drbg_random,&ctr,MBEDTLS_RSA_PRIVATE,nullptr,0,&olen,enc,plain,sizeof(plain));
  if(r!=0||olen<16)return false;
  memcpy(aesKey,plain,16); haveAes=true; mbedtls_aes_init(&aesDec); mbedtls_aes_setkey_dec(&aesDec,aesKey,128);
  return true;
}

void sendRtsp(int code,const String& req,const String& extra=""){
  String r="RTSP/1.0 "+String(code)+" "+(code==200?"OK":"Error")+"\r\n";
  r+="CSeq: "+String(cseqOf(req))+"\r\n";
  r+="Server: AirTunes/105.1\r\n";
  r+="Audio-Jack-Status: connected; type=analog\r\n";
  r+="Content-Length: 0\r\n";
  if(extra.length())r+=extra;
  r+="\r\n"; rtspClient.print(r);
}

void handleRtsp(String req,String body){
  int sp=req.indexOf(' '); String method=sp>0?req.substring(0,sp):"";
  String extra;
  String challenge=headerValue(req,"Apple-Challenge");
  if(challenge.length()){
    String ar=appleResponse(challenge); if(ar.length())extra+="Apple-Response: "+ar+"\r\n";
  }
  if(method=="OPTIONS"){
    extra+="Public: ANNOUNCE, SETUP, RECORD, PAUSE, FLUSH, TEARDOWN, OPTIONS, GET_PARAMETER, SET_PARAMETER\r\n";
    sendRtsp(200,req,extra); return;
  }
  if(method=="ANNOUNCE"){
    String key=headerValue(body,"rsaaeskey"); if(key.length())decryptAesKey(key);
    String iv=headerValue(body,"aesiv"); if(iv.length()){uint8_t x[32];int n=b64(iv,x,sizeof(x));if(n==16)memcpy(aesIv,x,16);}
    if(!parseFmtp(body) || !haveAes){ sendRtsp(400,req); return; }
    sendRtsp(200,req); return;
  }
  if(method=="SETUP"){
    remoteIp=rtspClient.remoteIP();
    String tr=headerValue(req,"Transport");
    int p=tr.indexOf("control_port="); if(p>=0)remoteControlPort=atoi(tr.c_str()+p+13);
    p=tr.indexOf("timing_port="); if(p>=0)remoteTimingPort=atoi(tr.c_str()+p+12);
    audioUdp.begin(audioPort); controlUdp.begin(controlPort); timingUdp.begin(timingPort);
    extra="Transport: RTP/AVP/UDP;unicast;mode=record;server_port="+String(audioPort)+";control_port="+String(controlPort)+";timing_port="+String(timingPort)+"\r\nSession: 1\r\n";
    sendRtsp(200,req,extra); return;
  }
  if(method=="RECORD"){
    String rtpInfo=headerValue(req,"RTP-Info");
    int p=rtpInfo.indexOf("seq=");
    if(p>=0) lastSeq=(uint16_t)atoi(rtpInfo.c_str()+p+4);
    p=rtpInfo.indexOf("rtptime=");
    if(p>=0){ lastRtptime=(uint32_t)strtoul(rtpInfo.c_str()+p+8,nullptr,10); recordRtpTime=lastRtptime; }
    recording=true; extra="Audio-Latency: 11025\r\n"; sendRtsp(200,req,extra); return;
  }
  if(method=="FLUSH"){while(audioUdp.parsePacket()>0){uint8_t d[8];audioUdp.read(d,sizeof(d));} if(pcmSink){} sendRtsp(200,req); return;}
  if(method=="TEARDOWN"){recording=false; sendRtsp(200,req); rtspClient.stop(); return;}
  if(method=="SET_PARAMETER"||method=="GET_PARAMETER"||method=="PAUSE"){sendRtsp(200,req); return;}
  sendRtsp(200,req);
}

uint64_t ntpNow(){
  const uint64_t unixSec=(uint64_t)(esp_timer_get_time()/1000000ULL);
  const uint64_t usec=(uint64_t)(esp_timer_get_time()%1000000ULL);
  const uint64_t sec=unixSec+2208988800ULL;
  const uint64_t frac=(usec<<32)/1000000ULL;
  return (sec<<32)|frac;
}

void processTiming(){
  int n=timingUdp.parsePacket();
  if(n!=32)return;
  uint8_t req[32];
  int got=timingUdp.read(req,sizeof(req));
  if(got!=32||req[0]!=0x80||((req[1]&0x7f)!=0x52)||remoteTimingPort==0)return;

  uint8_t resp[32]={};
  resp[0]=req[0];
  resp[1]=(uint8_t)((req[1]&0x80)|0x53);
  resp[2]=req[2]; resp[3]=req[3];
  // bytes 4..7 are the zero padding field.
  memcpy(resp+8,req+24,8); // reference/originate timestamp
  uint64_t now=ntpNow();
  for(int i=0;i<8;i++) resp[16+i]=(uint8_t)(now>>(56-8*i));
  now=ntpNow();
  for(int i=0;i<8;i++) resp[24+i]=(uint8_t)(now>>(56-8*i));

  timingUdp.beginPacket(remoteIp,remoteTimingPort);
  timingUdp.write(resp,sizeof(resp));
  timingUdp.endPacket();
}

void processAudio(){
  if(!recording||!audioUdp.parsePacket())return;
  static uint8_t pkt[1600]; int n=audioUdp.read(pkt,sizeof(pkt));
  if(n<12||((pkt[1]&0x7f)!=audioPayloadType))return;
  lastSeq=((uint16_t)pkt[2]<<8)|pkt[3]; lastRtptime=((uint32_t)pkt[4]<<24)|((uint32_t)pkt[5]<<16)|((uint32_t)pkt[6]<<8)|pkt[7];
  int plen=n-12; uint8_t* payload=pkt+12;
  if(haveAes){
    int encLen=plen&~15; if(encLen>0){unsigned char iv[16];memcpy(iv,aesIv,16);mbedtls_aes_crypt_cbc(&aesDec,MBEDTLS_AES_DECRYPT,encLen,iv,payload,payload); }
  }
  if(!alac||!pcmSink)return;
  static int16_t pcm[352*2]; int out=0; alac_decode_frame(alac,payload,pcm,&out);
  if(out>0)pcmSink((uint8_t*)pcm,out);
}

void advertise(){
  uint8_t mac[6];esp_read_mac(mac,ESP_MAC_WIFI_STA);
  char inst[64];snprintf(inst,sizeof(inst),"%02X%02X%02X%02X%02X%02X@C3 Music",mac[0],mac[1],mac[2],mac[3],mac[4],mac[5]);
  mdns_txt_item_t txt[]={
    {(char*)"txtvers",(char*)"1"},{(char*)"ch",(char*)"2"},{(char*)"cn",(char*)"0,1"},
    {(char*)"et",(char*)"0,1"},{(char*)"md",(char*)"0,1,2"},{(char*)"pw",(char*)"false"},
    {(char*)"sr",(char*)"44100"},{(char*)"ss",(char*)"16"},{(char*)"tp",(char*)"UDP"},
    {(char*)"vn",(char*)"3"},{(char*)"vs",(char*)"130.14"},{(char*)"am",(char*)"AirPort4,107"}
  };
  mdns_service_add(nullptr,"_raop","_tcp",rtspPort,txt,sizeof(txt)/sizeof(txt[0]));
  mdns_service_instance_name_set("_raop","_tcp",inst);
  Serial.printf("[AIRPLAY1] advertising %s:%u\n",inst,rtspPort);
}
}

void airplay1SetPcmSink(Airplay1PcmSink sink){pcmSink=sink;}

bool airplay1Start(uint16_t port){
  if(running)return true; if(WiFi.status()!=WL_CONNECTED)return false;
  rtspPort=port; if(!rsaInit())Serial.println("[AIRPLAY1] RSA init failed");
  rtspServer=WiFiServer(rtspPort);rtspServer.begin();rtspServer.setNoDelay(true);
  advertise();running=true;recording=false;
  Serial.printf("[AIRPLAY1] started, free heap=%u\n",ESP.getFreeHeap()); return true;
}

void processControl(){
  while(controlUdp.parsePacket()>0){
    uint8_t pkt[256];
    int n=controlUdp.read(pkt,sizeof(pkt));
    if(n<2) continue;
    uint8_t type=pkt[1]&0x7f;
    if(type==0x54){
      // RAOP sync packet. It is an anchor for the sender's RTP/NTP clock;
      // no response is required, but it must be drained promptly.
      if(n>=20) lastRtptime=((uint32_t)pkt[4]<<24)|((uint32_t)pkt[5]<<16)|((uint32_t)pkt[6]<<8)|pkt[7];
    } else if(type==0x55 || type==0x56){
      // Retransmission request/response. Keep the control socket drained so
      // control traffic can never starve the audio receiver.
    }
  }
}

void airplay1Loop(){
  if(!running)return;
  if(!rtspClient||!rtspClient.connected()){
    recording=false; if(rtspClient)rtspClient.stop();
    WiFiClient c=rtspServer.accept();
    if(c){rtspClient=c;rtspClient.setTimeout(50);Serial.println("[AIRPLAY1] RTSP client connected");}
  }

  // Never block the main loop for hundreds of milliseconds while streaming.
  if(rtspClient&&rtspClient.connected()&&rtspClient.available()){
    String req=rtspClient.readStringUntil('\n'); req+="\n";
    uint32_t end=millis()+25;
    while(millis()<end){
      if(!rtspClient.available()) break;
      String line=rtspClient.readStringUntil('\n');
      req+=line;
      if(line=="\r\n"||line=="\n") break;
    }
    int content=headerValue(req,"Content-Length").toInt();
    String body;
    while((int)body.length()<content && rtspClient.available())
      body+=(char)rtspClient.read();
    if((int)body.length()==content) handleRtsp(req,body);
  }

  processTiming();
  processControl();

  // Drain a small burst of queued RTP packets each pass. At 44.1 kHz,
  // 352-sample ALAC frames arrive about every 8 ms; processing only one
  // packet per loop can otherwise build latency and eventually underrun.
  for(int i=0;i<8 && recording;i++){
    if(!audioUdp.parsePacket()) break;
    processAudio();
  }
}

void airplay1Stop(){
  if(!running)return; recording=false; if(rtspClient)rtspClient.stop();
  rtspServer.stop();audioUdp.stop();controlUdp.stop();timingUdp.stop();mdns_service_remove("_raop","_tcp");
  if(alac){alac_free(alac);alac=nullptr;} if(haveAes){mbedtls_aes_free(&aesDec);haveAes=false;}
  running=false;Serial.println("[AIRPLAY1] stopped");
}

bool airplay1IsRunning(){return running;}
