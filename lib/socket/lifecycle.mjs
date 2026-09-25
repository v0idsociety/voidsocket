import { createHash, randomBytes } from 'node:crypto';
import { ERR_CODES, GUID, makeCloseFrame } from '../../shared.mjs';
import RingQueue from './ring-queue.mjs';
export function _onClose(code=1006){
if(this.isClosed)return;
this.isClosed=true;this.isOpen=false;this.saturated=false;
this._setState('disconnected');this._stopPing();this.clearAllTimeouts();
if(this._timer){clearTimeout(this._timer);this._timer=null;}
this._frags=null;this._fragOp=0;this._fragBytes=0;this._fragCount=0;this._rbLen=0;this._flushing=false;
if(this._rlQueue.length>0){
if(this._reconnect&&!this._killed){
const nq=new RingQueue(this._rlQueue.length+this._queue.length+1);
while(this._rlQueue.length>0){const it=this._rlQueue.shift();it.cb=null;nq.push(it.data);}
while(this._queue.length>0)nq.push(this._queue.shift());
this._queue=nq;
}else{while(this._rlQueue.length>0){const it=this._rlQueue.shift();if(it.cb)it.cb(new Error('connection closed'));it.cb=null;}}
this._rlQueue.clear();}
if(this._sock){
if(!this._sock.destroyed){try{this._sock.write(makeCloseFrame(code,!this.isServer));}catch{ }this._sock.destroy();}
this._sock.removeAllListeners('data');this._sock.removeAllListeners('error');this._sock.removeAllListeners('close');this._sock.removeAllListeners('drain');
this._sock=null;}
if(this._ackWaiters.length>0){
const err=new Error('connection closed');
for(let i=0;i<this._ackWaiters.length;i++){clearTimeout(this._ackWaiters[i].timer);this._ackWaiters[i].timer=null;this._ackWaiters[i].reject(err);}
this._ackWaiters.length=0;}
if(this._sentAcks.size>0)this._sentAcks.clear();
this._fire(this.offline,code);
if(this._reconnect&&!this._killed&&this.reconnectAttempts<this.maxReconnectAttempts){
this._reconnectCount++;
if(this._urls&&this.reconnectAttempts>0&&this.reconnectAttempts%this._urls.length===0)this._nextUrl();
const base=this._adaptiveDelay?this.reconnectDelay*(2-this._qualityScore/100):this.reconnectDelay;
const delay=Math.min(base*Math.pow(1.5,this.reconnectAttempts),this.maxReconnectDelay)+((Math.random()*200)|0);
this.reconnectAttempts++;this._setState('reconnecting');this._fire(this.reconnecting,this.reconnectAttempts,delay);
this._reconnectTimer=setTimeout(()=>{
this._reconnectTimer=null;
if(this._killed)return;
this._key=randomBytes(16).toString('base64');this._expect=createHash('sha1').update(this._key+GUID).digest('base64');
this.isOpen=false;this.isClosed=false;
this._minLatency=Infinity;this._maxLatency=0;this._totalLatency=0;this._pingCount=0;this._qualityScore=100;this._healthFailures=0;
this._frags=null;this._fragOp=0;this._fragBytes=0;this._fragCount=0;
this._init();},delay);
if(this._reconnectTimer.unref)this._reconnectTimer.unref();}}
export function reconnect(){
if(this._killed){this._throwError(ERR_CODES.ERR_ALREADY_KILLED,'Cannot reconnect on a killed socket');return;}
if(this._reconnectTimer){clearTimeout(this._reconnectTimer);this._reconnectTimer=null;}
this._killed=false;this._onClose(1000);this.isClosed=false;
this._key=randomBytes(16).toString('base64');this._expect=createHash('sha1').update(this._key+GUID).digest('base64');
this._frags=null;this._fragOp=0;this._fragBytes=0;this._fragCount=0;
this._init();}
export function close(code=1000){
if(this._killed){this._throwError(ERR_CODES.ERR_ALREADY_KILLED,'Socket already killed');return;}
if(this.isClosed&&!this.isOpen){this._throwError(ERR_CODES.ERR_ALREADY_CLOSED,'Socket already closed');return;}
this._killed=true;this._reconnect=false;
if(this._reconnectTimer){clearTimeout(this._reconnectTimer);this._reconnectTimer=null;}
this._stopPing();this.clearAllTimeouts();this._onClose(code);}
export function kill(){
if(this._killed){this._throwError(ERR_CODES.ERR_ALREADY_KILLED,'Socket already killed');return;}
this._killed=true;this._setState('killed');
if(this._reconnectTimer){clearTimeout(this._reconnectTimer);this._reconnectTimer=null;}
if(this._rlTimer){clearInterval(this._rlTimer);this._rlTimer=null;}
this.clearAllTimeouts();this._stopPing();this._stopHealthCheck();
if(this._sock&&!this._sock.destroyed)this._sock.destroy();
this._onClose(1000);}
