import { inflateRawSync } from 'node:zlib';
import { ERR_CODES } from '../../shared.mjs';
const toMatcher=m=>typeof m==='function'?m:typeof m==='string'?buf=>buf.includes(m):typeof m==='number'?(_,op)=>op===m:m instanceof RegExp?buf=>m.test(buf.toString()):()=>true;
export function _checkAck(payload,op){
const len=this._ackWaiters.length;
if(len===0)return;
for(let i=0;i<len;i++){const w=this._ackWaiters[i];
if(w.match(payload,op)){const last=len-1;if(i!==last)this._ackWaiters[i]=this._ackWaiters[last];this._ackWaiters.length=last;clearTimeout(w.timer);w.timer=null;w.resolve(payload);break;}}}
export function _emitRecv(payload,op){
if(this._compressEnabled&&Buffer.isBuffer(payload)&&payload.length>0&&payload[0]===1){try{payload=inflateRawSync(payload.subarray(1));}catch{}}
if(this.afterRecv){payload=this.afterRecv(payload,op);if(payload===false||payload==null)return;}
if(this._ackWaiters.length>0)this._checkAck(payload,op);
if(!this.recv)return;
const h=this.recv;
if(typeof h==='function')this._json&&op===1?recvJson(h,payload,op):h(payload,op);
else for(let i=0;i<h.length;i++)this._json&&op===1?recvJson(h[i],payload,op):h[i](payload,op);}
function recvJson(fn,payload,op){try{fn(JSON.parse(payload),op);}catch(e){fn(payload,op);}}
export function sendAck(data,matcher,opts={}){
if(this._killed)return Promise.reject(this._throwError(ERR_CODES.ERR_ALREADY_KILLED,'Cannot use sendAck on a killed socket'));
if(!this.isOpen||!this._sock||this._sock.destroyed)return Promise.reject(this._throwError(ERR_CODES.ERR_NOT_OPEN,'Cannot use sendAck: socket is not open'));
const timeout=opts.timeout??5000,retries=opts.retries??3,matchFn=toMatcher(matcher);
return new Promise((resolve,reject)=>{
let attempts=0;
const trySend=()=>{
attempts++;
if(!this.isOpen||!this._sock||this._sock.destroyed)return reject(this._throwError(ERR_CODES.ERR_NOT_OPEN,'Connection lost during sendAck'));
this.send(data);
const waiter={match:matchFn,resolve,reject,timer:setTimeout(()=>{
const idx=this._ackWaiters.indexOf(waiter);
if(idx!==-1)this._ackWaiters.splice(idx,1);
waiter.timer=null;
if(attempts<retries&&this.isOpen&&this._sock&&!this._sock.destroyed)trySend();else reject(new Error('ack timeout'));},timeout)};
this._ackWaiters.push(waiter);};
trySend();});}
export function expectAck(matcher,timeout=5000){
if(this._killed)return Promise.reject(this._throwError(ERR_CODES.ERR_ALREADY_KILLED,'Cannot use expectAck on a killed socket'));
if(!this.isOpen||!this._sock||this._sock.destroyed)return Promise.reject(this._throwError(ERR_CODES.ERR_NOT_OPEN,'Cannot use expectAck: socket is not open'));
const matchFn=toMatcher(matcher);
return new Promise((resolve,reject)=>{
const waiter={match:matchFn,resolve,reject,timer:setTimeout(()=>{
const idx=this._ackWaiters.indexOf(waiter);
if(idx!==-1)this._ackWaiters.splice(idx,1);
waiter.timer=null;reject(new Error('ack timeout'));},timeout)};
this._ackWaiters.push(waiter);});}
