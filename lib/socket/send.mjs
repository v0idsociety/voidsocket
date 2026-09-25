import { ERR_CODES, pack, frameLen, packInto, getFrameBuf, releaseFrameBuf, usePool } from '../../shared.mjs';
export function writePooled(sock,fb,total,cb){
const view=fb.subarray(0,total);
try{
if(cb)return sock.write(view,err=>{releaseFrameBuf(fb);cb(err);});
return sock.write(view,()=>releaseFrameBuf(fb));
}catch(e){releaseFrameBuf(fb);throw e;}}
export function _enqueue(data){
if(this._maxQueueSize!==Infinity&&this._queue.length>=this._maxQueueSize){this._queue.shift();this._fire(this.error,new Error(`queue full: dropped oldest message (cap ${this._maxQueueSize})`));}
this._queue.push(Buffer.isBuffer(data)?Buffer.from(data):data);}
export function _flushQueue(){
if(this._queue.length===0||this._flushing)return;
if(!this.isOpen||!this._sock||this._sock.destroyed)return;
this._flushing=true;
const sock=this._sock;
sock.cork();
while(this._queue.length>0){if(!this.isOpen||!this._sock||this._sock.destroyed)break;this._sendDirect(this._queue.shift());if(this.saturated)break;}
sock.uncork();
if(this._queue.length>0&&this.isOpen&&this._sock&&!this._sock.destroyed&&this.saturated)this._sock.once('drain',()=>{this.saturated=false;this._flushing=false;this._flushQueue();});
else this._flushing=false;}
export function cork(){if(!this._sock||this._sock.destroyed){this._throwError(ERR_CODES.ERR_NO_SOCKET,'Cannot cork: no active socket');return;}this._corked=true;this._sock.cork();}
export function uncork(){if(!this._sock||this._sock.destroyed){this._throwError(ERR_CODES.ERR_NO_SOCKET,'Cannot uncork: no active socket');return;}if(!this._corked){this._throwError(ERR_CODES.ERR_INVALID_STATE,'Cannot uncork: socket is not corked');return;}this._corked=false;this._sock.uncork();}
export function _sendDirect(data,cb){
if(!this.isOpen||!this._sock||this._sock.destroyed){const err=this._throwError(ERR_CODES.ERR_NOT_OPEN,'Socket is not open');if(cb)cb(err);return false;}
if(this.beforeSend){const r=this.beforeSend(data);if(r===false)return false;if(r!==void 0&&r!==true)data=r;}
let payload,op;
if(typeof data==='string'){payload=data;op=1;}
else if(Buffer.isBuffer(data)){payload=data;op=2;}
else if(data!==null&&typeof data==='object'&&!ArrayBuffer.isView(data)){payload=Buffer.from(JSON.stringify(data));op=1;}
else{payload=Buffer.from(data.buffer||data,data.byteOffset||0,data.byteLength||0);op=2;}
if(this._compressEnabled){const c=this._compressPayload(payload);if(c!==payload)payload=c;}
const mask=!this.isServer,total=frameLen(payload,mask);
let ok;
if(usePool(total)){const fb=getFrameBuf(total);packInto(fb,op,payload,mask,this.fast);ok=writePooled(this._sock,fb,total,cb);}
else ok=this._sock.write(pack(op,payload,mask,this.fast),cb);
if(!ok)this.saturated=true;
if(this._statsEnabled){this._msgOut++;this._bytesOut+=total;}
return ok;}
export function send(data,cb){
if(!this.isOpen||!this._sock||this._sock.destroyed)return this._killed?(cb?.(this._throwError(ERR_CODES.ERR_ALREADY_KILLED,'Cannot send on a killed socket')),false):this._queueEnabled?(this._enqueue(data),false):(cb?.(this._throwError(ERR_CODES.ERR_NOT_OPEN,'Socket is not open')),false);
if(this._rlEnabled&&!this._rlCheck(data,cb))return false;
return this._sendDirect(data,cb);}
export function raw(buf,cb){
if(this.isOpen&&this._sock&&!this._sock.destroyed){const ok=this._sock.write(buf,cb);if(!ok)this.saturated=true;if(this._statsEnabled){this._msgOut++;this._bytesOut+=buf.length;}return ok;}
if(this._killed){if(cb)cb(this._throwError(ERR_CODES.ERR_ALREADY_KILLED,'Cannot use raw on a killed socket'));return false;}
if(cb)cb(this._throwError(ERR_CODES.ERR_NOT_OPEN,'Socket is not open'));return false;}
export function json(obj,cb){
if(!this.isOpen||!this._sock||this._sock.destroyed){
if(this._killed){if(cb)cb(this._throwError(ERR_CODES.ERR_ALREADY_KILLED,'Cannot use json on a killed socket'));return false;}
if(this._queueEnabled){this._enqueue(obj);return false;}
if(cb)cb(this._throwError(ERR_CODES.ERR_NOT_OPEN,'Socket is not open'));return false;}
if(this.beforeSend){const r=this.beforeSend(obj);if(r===false)return false;if(r!==undefined&&r!==true)obj=r;}
const mask=!this.isServer,str=JSON.stringify(obj),total=frameLen(str,mask);
let ok;
if(usePool(total)){const fb=getFrameBuf(total);packInto(fb,1,str,mask,this.fast);ok=writePooled(this._sock,fb,total,cb);}
else ok=this._sock.write(pack(1,str,mask,this.fast),cb);
if(!ok)this.saturated=true;
if(this._statsEnabled){this._msgOut++;this._bytesOut+=total;}
return ok;}
export function sendBatch(messages,cb){
if(!this.isOpen||!this._sock||this._sock.destroyed){
if(this._killed){if(cb)cb(this._throwError(ERR_CODES.ERR_ALREADY_KILLED,'Cannot use sendBatch on a killed socket'));return false;}
if(this._queueEnabled){for(const m of messages)this._enqueue(m);return false;}
if(cb)cb(this._throwError(ERR_CODES.ERR_NOT_OPEN,'Socket is not open'));return false;}
if(this._sock.cork)this._sock.cork();
let ok=true;
for(const m of messages)if(!this._sendDirect(m))ok=false;
if(this._sock.uncork)this._sock.uncork();
if(cb)cb(ok?null:new Error('backpressure'));return ok;}
export function broadcastTo(filter,data){
if(!this.isServer){this._throwError(ERR_CODES.ERR_NOT_SERVER,'broadcastTo can only be called on a server socket');return false;}
if(!this.isOpen||!this._sock||this._sock.destroyed){if(this._killed){this._throwError(ERR_CODES.ERR_ALREADY_KILLED,'Cannot broadcast on a killed socket');return false;}return false;}
if(filter&&!filter(this))return false;
return this._sendDirect(data);}
export function flush(){if(!this.saturated||!this._sock||this._sock.destroyed)return Promise.resolve();return new Promise(r=>this._sock.once('drain',r));}
