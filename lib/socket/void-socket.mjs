import { createHash, randomBytes } from 'node:crypto';
import { connect as tlsConnect } from 'node:tls';
import { connect as netConnect } from 'node:net';
import { GUID, DEFAULT_CIPHERS } from '../../shared.mjs';
import RingQueue from './ring-queue.mjs';
import * as events from './events.mjs';
import * as meta from './meta.mjs';
import * as timers from './timers.mjs';
import * as compression from './compression.mjs';
import * as ping from './ping.mjs';
import * as rateLimit from './rate-limit.mjs';
import * as ack from './ack.mjs';
import * as send from './send.mjs';
import * as lifecycle from './lifecycle.mjs';
import * as receive from './receive.mjs';
class VoidSocket{
static _idCounter=0;
constructor(url,opts={}){
if(typeof url==='object'&&url!==null&&!Array.isArray(url)&&!(url instanceof URL)&&!url.href){opts=url;url=opts.url||null;}
this._uid=++VoidSocket._idCounter;this._name=opts.name||null;
this.isServer=opts.isServer===true||!!opts.socket;this.opts=opts;
this.fast=opts.fastMask??true;this.autoPong=opts.autoPong!==false;this.saturated=false;
this.recv=null;this.online=null;this.offline=null;this.error=null;this.onPing=null;this.flushed=null;this.reconnecting=null;this.connecting=null;this.onStateChange=null;
this.beforeSend=null;this.afterRecv=null;
this._maxPayload=Math.max(0,opts.maxPayload??(1024*1024*1024));
this._maxFragments=Math.max(0,opts.maxFragments??0);
this._highWaterMark=Math.max(1024,opts.highWaterMark??(2*1024*1024*1024));
this._rbInitSize=Math.max(1024,opts.bufferSize||65536);
this._rb=Buffer.allocUnsafe(this._rbInitSize);this._rbLen=0;this._rbOff=0;this._flushing=false;
this._frags=null;this._fragOp=0;this._fragBytes=0;this._fragCount=0;
this._ackWaiters=[];
this._pingInterval=opts.pingInterval??30000;this._pingTimeout=opts.pingTimeout??10000;this._autoPing=opts.autoPing!==false;
this._pingTimer=null;this._pongTimer=null;this._awaitingPong=false;
this._json=opts.json===true;
this._statsEnabled=opts.stats!==false;
this._msgIn=0;this._msgOut=0;this._bytesIn=0;this._bytesOut=0;this._reconnectCount=0;this._connectedAt=0;this._lastLatency=0;this._pingSentAt=0;
this._minLatency=Infinity;this._maxLatency=0;this._totalLatency=0;this._pingCount=0;
this._msgId=0;this._sentAcks=new Map();this._meta={};
this._state='idle';this._healthTimer=null;this._healthCheck=opts.healthCheck===true;
this._healthInterval=opts.healthInterval??30000;this._healthTimeout=opts.healthTimeout??10000;
this._lastHealthCheck=0;this._healthFailures=0;this._maxHealthFailures=opts.maxHealthFailures??5;
this._adaptiveDelay=opts.adaptiveReconnect===true;this._qualityScore=100;
this._compressEnabled=opts.compress===true;this._compressThreshold=opts.compressThreshold??1024;this._compressLevel=opts.compressLevel??1;
this._compressSent=0;this._compressReceived=0;
this._autoTimeouts=new Map();this._corked=false;this._killed=false;
const rl=opts.rateLimit;
this._rlEnabled=!!rl;this._rlCount=rl?rl.count:0;this._rlWindow=rl?rl.window:0;this._rlTokens=rl?rl.count:0;
this._rlTimer=null;this._rlQueue=new RingQueue(64);
if(this._rlEnabled){this._rlTimer=setInterval(()=>{this._rlTokens=this._rlCount;this._drainRlQueue();},this._rlWindow);if(this._rlTimer.unref)this._rlTimer.unref();}
if(this.isServer){
this._reconnect=false;this.reconnectAttempts=0;this.maxReconnectAttempts=0;this.reconnectDelay=0;this.maxReconnectDelay=0;
this._queueEnabled=false;this._maxQueueSize=0;this._queue=new RingQueue();
this.req=opts.req||null;
this.ip=opts.req?(opts.req.headers['x-forwarded-for']?.split(',')[0]?.trim()||opts.req.socket?.remoteAddress||''):(opts.socket?.remoteAddress||'');
this.url=typeof url==='string'?new URL(url):(url||(opts.req?new URL(opts.req.url,'http://localhost'):null));
this.isOpen=true;this.isClosed=false;this._connectedAt=Date.now();
this._sock=opts.socket;
this._sock.setNoDelay(this.opts.noDelay!==false);
if(this.opts.keepAlive!==false)this._sock.setKeepAlive(true,this.opts.keepAliveInterval||10000);
const frag=this.opts.maxSendFragment??2048;
if(frag&&this._sock.setMaxSendFragment)this._sock.setMaxSendFragment(frag);
this._sock.setTimeout(0);
this._sock.on('data',d=>this._onData(d));
this._sock.on('error',e=>{this._fire(this.error,e);if(!this.isClosed)this._onClose(1006);});
this._sock.on('end',()=>this._onClose());this._sock.on('close',()=>this._onClose());
this._sock.on('drain',()=>{this.saturated=false;this._fire(this.flushed);});
this._startPing();
if(opts.head&&Buffer.isBuffer(opts.head)&&opts.head.length>0){const h=opts.head;process.nextTick(()=>{if(this.isOpen&&this._sock&&!this._sock.destroyed)this._onData(h);});}
process.nextTick(()=>{if(this.isOpen)this._fire(this.online);});
}else{
const deferErr=e=>queueMicrotask(()=>this._fire(this.error,e));
const toUrl=u=>{
if(u instanceof URL){if(!u.protocol||(u.protocol!=='ws:'&&u.protocol!=='wss:')){deferErr(new Error('Invalid WebSocket protocol'));return null;}return u;}
try{const p=new URL(u);if(!p.protocol||(p.protocol!=='ws:'&&p.protocol!=='wss:'))throw new Error('x');return p;}
catch(e){deferErr(new Error('Invalid URL: '+u));return null;}};
if(Array.isArray(url)){this._urls=url.map(u=>typeof u==='string'||u instanceof URL?toUrl(u):(deferErr(new Error('Invalid URL type')),null)).filter(u=>u!==null);this._urlIndex=0;this.url=this._urls.length>0?this._urls[0]:null;}
else{this._urls=null;this._urlIndex=0;
if(typeof url==='string'||url instanceof URL)this.url=toUrl(url);
else{deferErr(new Error('Invalid URL type'));this.url=null;}}
this.isOpen=false;this.isClosed=false;
this._reconnect=opts.reconnect!==false;this.reconnectAttempts=0;
this.maxReconnectAttempts=Math.max(0,opts.maxReconnectAttempts??Infinity);
this.reconnectDelay=Math.max(50,opts.reconnectDelay??500);
this.maxReconnectDelay=Math.max(100,opts.maxReconnectDelay??10000);
this._queueEnabled=opts.queue!==false;this._maxQueueSize=opts.maxQueueSize??512;this._queue=new RingQueue();
this._sock=null;
this._key=randomBytes(16).toString('base64');this._expect=createHash('sha1').update(this._key+GUID).digest('base64');
this._timer=null;this._reconnectTimer=null;
if(!this.url){deferErr(new Error('No valid URL provided'));this.isClosed=true;return;}
this._init();}}
get stats(){return{messagesIn:this._msgIn,messagesOut:this._msgOut,bytesIn:this._bytesIn,bytesOut:this._bytesOut,latency:this._lastLatency,minLatency:this._minLatency===Infinity?0:this._minLatency,maxLatency:this._maxLatency,avgLatency:this._pingCount>0?Math.round(this._totalLatency/this._pingCount):0,pingCount:this._pingCount,uptime:this._connectedAt>0?Date.now()-this._connectedAt:0,reconnects:this._reconnectCount,queueSize:this._queue.length,saturated:this.saturated,messageId:this._msgId,meta:this._meta,id:this._uid,name:this._name,state:this._state,qualityScore:this._qualityScore,healthFailures:this._healthFailures,lastHealthCheck:this._lastHealthCheck,compressionEnabled:this._compressEnabled,compressSent:this._compressSent,compressReceived:this._compressReceived};}
_init(){
if(!this.url){this._fire(this.error,new Error('Cannot initialize: no valid URL'));this._onClose(1006);return;}
const s=this.url.protocol==='wss:';
const host=this.opts.host||this.url.hostname,port=parseInt(this.opts.port||this.url.port)||(s?443:80),path=(this.url.pathname||'/')+(this.url.search||'');
const hostHdr=this.opts.headers?.Host||this.opts.headers?.host||this.opts.servername||this.url.hostname;
const parts=[`GET ${path} HTTP/1.1`,`Host: ${hostHdr}`,'Upgrade: websocket','Connection: Upgrade',`Sec-WebSocket-Key: ${this._key}`,'Sec-WebSocket-Version: 13'];
if(this.opts.headers)for(const[k,v]of Object.entries(this.opts.headers)){const l=k.toLowerCase();if(l!=='host'&&l!=='upgrade'&&l!=='connection'&&l!=='sec-websocket-key'&&l!=='sec-websocket-version')parts.push(`${k}: ${v}`);}
if(this.opts.protocols)parts.push(`Sec-WebSocket-Protocol: ${Array.isArray(this.opts.protocols)?this.opts.protocols.join(', '):this.opts.protocols}`);
const reqBuf=Buffer.from(parts.join('\r\n')+'\r\n\r\n');
const fail=e=>{this._fire(this.error,e);this._onClose(1006);};
const setupSocket=(sock,msg)=>{this._sock=sock;if(!sock){fail(new Error(msg));return false;}return true;};
const armSock=ev=>{this._sock.once(ev,()=>{if(this._sock&&!this._sock.destroyed)this._sock.write(reqBuf);});};
if(this.opts.createSocket){
try{const sock=this.opts.createSocket(this.url);if(!sock){fail(new Error('createSocket returned null'));return;}this._sock=sock;
const w=()=>{if(this._sock&&!this._sock.destroyed)this._sock.write(reqBuf);};
if(this._sock.connecting)this._sock.once(s?'secureConnect':'connect',w);else w();}
catch(e){fail(e);return;}}
else if(s){
try{if(!setupSocket(tlsConnect({host,port,servername:this.opts.servername||this.url.hostname,rejectUnauthorized:this.opts.rejectUnauthorized??true,minVersion:this.opts.minVersion||'TLSv1.2',ciphers:this.opts.ciphers||DEFAULT_CIPHERS,ALPNProtocols:this.opts.ALPNProtocols||['http/1.1'],...(this.opts.secureContext?{secureContext:this.opts.secureContext}:{}),...(this.opts.maxVersion?{maxVersion:this.opts.maxVersion}:{}),...(this.opts.session?{session:this.opts.session}:{}),...(this.opts.tls||{})}),'Failed to create TLS socket'))return;armSock('secureConnect');}
catch(e){fail(e);return;}}
else{
try{if(!setupSocket(netConnect({host,port}),'Failed to create socket'))return;armSock('connect');}
catch(e){fail(e);return;}}
this._sock.setNoDelay(this.opts.noDelay!==false);
if(this.opts.keepAlive!==false)this._sock.setKeepAlive(true,this.opts.keepAliveInterval||10000);
const frag=this.opts.maxSendFragment??2048;
if(frag&&this._sock.setMaxSendFragment)this._sock.setMaxSendFragment(frag);
this._sock.setTimeout(0);
this._sock.on('data',d=>this._onData(d));
this._fire(this.connecting);this._setState('connecting');
this._sock.on('error',e=>{this._fire(this.error,e);if(!this.isClosed)this._onClose(1006);});
this._sock.on('end',()=>this._onClose());this._sock.on('close',()=>this._onClose());
this._sock.on('drain',()=>{this.saturated=false;this._fire(this.flushed);});
const to=this.opts.handshakeTimeout??this.opts.connectTimeout??5000;
if(to>0){this._timer=setTimeout(()=>{if(!this.isOpen){this._fire(this.error,new Error('timeout'));this._onClose(1006);}},to);if(this._timer.unref)this._timer.unref();}}
get isConnected(){return this.isOpen;}
get latency(){return this._lastLatency;}
get id(){return this._msgId;}
get uid(){return this._uid;}
get name(){return this._name;}
get state(){return this._state;}
get quality(){return this._qualityScore;}
get meta(){return this._meta;}
get socket(){return this._sock;}
get bufferedAmount(){return this._sock?this._sock.writableLength:0;}}
Object.assign(VoidSocket.prototype,{
_throwError:events._throwError,_fire:events._fire,_evKey:events._evKey,on:events.on,off:events.off,once:events.once,
setMeta:meta.setMeta,getMeta:meta.getMeta,hasMeta:meta.hasMeta,removeMeta:meta.removeMeta,_setState:meta._setState,setName:meta.setName,
setAutoTimeout:timers.setAutoTimeout,clearAutoTimeout:timers.clearAutoTimeout,setTimeout:timers.setTimeout,clearTimeout:timers.clearTimeout,clearAllTimeouts:timers.clearAllTimeouts,
enableCompression:compression.enableCompression,disableCompression:compression.disableCompression,isCompressionEnabled:compression.isCompressionEnabled,getCompressionStats:compression.getCompressionStats,_compressPayload:compression._compressPayload,
_startPing:ping._startPing,_stopPing:ping._stopPing,_onPong:ping._onPong,ping:ping.ping,pong:ping.pong,sendPing:ping.sendPing,sendPong:ping.sendPong,_startHealthCheck:ping._startHealthCheck,_stopHealthCheck:ping._stopHealthCheck,_updateQuality:ping._updateQuality,
_rlCheck:rateLimit._rlCheck,_drainRlQueue:rateLimit._drainRlQueue,_nextUrl:rateLimit._nextUrl,
_checkAck:ack._checkAck,_emitRecv:ack._emitRecv,sendAck:ack.sendAck,expectAck:ack.expectAck,
_enqueue:send._enqueue,_flushQueue:send._flushQueue,cork:send.cork,uncork:send.uncork,_sendDirect:send._sendDirect,send:send.send,raw:send.raw,json:send.json,sendBatch:send.sendBatch,broadcastTo:send.broadcastTo,flush:send.flush,
_onClose:lifecycle._onClose,reconnect:lifecycle.reconnect,close:lifecycle.close,kill:lifecycle.kill,
_pushRb:receive._pushRb,_onData:receive._onData,_parseHandshake:receive._parseHandshake,});
receive.__setBasePing(VoidSocket.prototype.ping);
export default VoidSocket;
export { VoidSocket };
