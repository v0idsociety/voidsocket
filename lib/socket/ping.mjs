import { EMPTY, PONG_EMPTY, PONG_EMPTY_UNMASKED, PING_EMPTY, PING_EMPTY_UNMASKED, ERR_CODES, pack } from '../../shared.mjs';
export function _startPing(){
this._stopPing();
if(!this._autoPing||this._pingInterval<=0)return;
this._pingTimer=setInterval(()=>{
if(!this.isOpen||!this._sock||this._sock.destroyed)return;
this._awaitingPong=true;this._pingSentAt=Date.now();
this._sock.write(this.isServer?PING_EMPTY_UNMASKED:PING_EMPTY);
this._pongTimer=setTimeout(()=>{
if(this._awaitingPong){this._healthFailures++;this._fire(this.error,new Error('ping timeout'));this._onClose(1006);}},this._pingTimeout);
if(this._pongTimer.unref)this._pongTimer.unref();},this._pingInterval);
if(this._pingTimer.unref)this._pingTimer.unref();
this._startHealthCheck();}
export function _stopPing(){if(this._pingTimer){clearInterval(this._pingTimer);this._pingTimer=null;}if(this._pongTimer){clearTimeout(this._pongTimer);this._pongTimer=null;}this._awaitingPong=false;this._stopHealthCheck();}
export function _onPong(){
if(!this._awaitingPong)return;
this._awaitingPong=false;this._lastLatency=Date.now()-this._pingSentAt;
this._pingCount++;this._totalLatency+=this._lastLatency;
if(this._lastLatency<this._minLatency)this._minLatency=this._lastLatency;
if(this._lastLatency>this._maxLatency)this._maxLatency=this._lastLatency;
this._healthFailures=Math.max(0,this._healthFailures-1);
this._updateQuality();
if(this._pongTimer){clearTimeout(this._pongTimer);this._pongTimer=null;}}
export function ping(payload=EMPTY,cb){
if(this.isOpen&&this._sock&&!this._sock.destroyed){
const mask=!this.isServer;
if(!mask&&(!payload||payload.length===0))return this._sock.write(PING_EMPTY_UNMASKED,cb);
return this._sock.write(pack(9,payload,mask,this.fast),cb);}
if(this._killed){if(cb)cb(this._throwError(ERR_CODES.ERR_ALREADY_KILLED,'Cannot ping on a killed socket'));return false;}
if(cb)cb(this._throwError(ERR_CODES.ERR_NOT_OPEN,'Cannot ping: socket is not open'));return false;}
export function pong(payload=EMPTY,cb){
if(this.isOpen&&this._sock&&!this._sock.destroyed){
const mask=!this.isServer;
if(!mask&&(!payload||payload.length===0))return this._sock.write(PONG_EMPTY_UNMASKED,cb);
return this._sock.write(pack(10,payload,mask,this.fast),cb);}
if(this._killed){if(cb)cb(this._throwError(ERR_CODES.ERR_ALREADY_KILLED,'Cannot pong on a killed socket'));return false;}
if(cb)cb(this._throwError(ERR_CODES.ERR_NOT_OPEN,'Cannot pong: socket is not open'));return false;}
export function sendPing(payload,cb){return this.ping(payload,cb);}
export function sendPong(payload,cb){return this.pong(payload,cb);}
export function _startHealthCheck(){
this._stopHealthCheck();
if(!this._healthCheck||this._healthInterval<=0)return;
this._healthTimer=setInterval(()=>{
if(!this.isOpen||!this._sock||this._sock.destroyed)return;
this._lastHealthCheck=Date.now();
if(this._awaitingPong){this._healthFailures++;if(this._healthFailures>=this._maxHealthFailures){this._fire(this.error,new Error('health check failed'));this._onClose(1006);return;}}
this._updateQuality();},this._healthInterval);
if(this._healthTimer.unref)this._healthTimer.unref();}
export function _stopHealthCheck(){if(this._healthTimer){clearInterval(this._healthTimer);this._healthTimer=null;}}
export function _updateQuality(){
if(this._pingCount===0){this._qualityScore=100;return;}
const avg=this._totalLatency/this._pingCount,loss=this._healthFailures/Math.max(this._pingCount,1);
let s=100;
if(avg>100)s-=30;else if(avg>50)s-=15;else if(avg>20)s-=5;
s-=loss*50;
s-=this._reconnectCount*5;
this._qualityScore=Math.max(0,Math.min(100,Math.round(s)));}
