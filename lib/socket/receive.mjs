import { CRLF, EMPTY, PONG_EMPTY, PONG_EMPTY_UNMASKED, PING_EMPTY, PING_EMPTY_UNMASKED, mask32, pack } from '../../shared.mjs';
let _basePing=null;
export function __setBasePing(fn){_basePing=fn;}
function combine(frags,count,bytes){
if(count===1)return frags[0];
const out=Buffer.allocUnsafe(bytes);let o=0;
for(let i=0;i<frags.length;i++){const f=frags[i];if(f.length>0){out.set(f,o);o+=f.length;}}
return out;}
function sendPong(sock,isSrv,p,has,fast){if(!has){sock.write(isSrv?PONG_EMPTY_UNMASKED:PONG_EMPTY);return;}sock.write(pack(10,p,!isSrv,fast));}
function dispatchPing(inst,p){if(inst.onPing)inst._fire(inst.onPing,p);else if(inst.ping!==_basePing)inst.ping(p);}
function combineFin(inst){
const c=combine(inst._frags,inst._fragCount,inst._fragBytes);
inst._frags=null;
inst._emitRecv(c,inst._fragOp);}
export function _pushRb(chunk){
const cl=chunk.length,need=this._rbOff+this._rbLen+cl;
if(need>this._rb.length){
if(this._rb.length<this._highWaterMark){const nb=Buffer.allocUnsafe(Math.max(this._rb.length<<1,this._rbLen+cl));if(this._rbLen>0)this._rb.copy(nb,0,this._rbOff,this._rbOff+this._rbLen);this._rb=nb;this._rbOff=0;}
else return;}
chunk.copy(this._rb,this._rbOff+this._rbLen);this._rbLen+=cl;}
export function _onData(c){
if(!this.isOpen)return this._parseHandshake(c);
if(this._rbLen>0)this._pushRb(c);
const buf=this._rbLen>0?this._rb.subarray(this._rbOff,this._rbOff+this._rbLen):c,L=buf.length;
let o=0;
const isSrv=this.isServer,maskReq=isSrv&&this.opts.maskRequired!==false,sock=this._sock,autoPong=this.autoPong,alive=sock&&!sock.destroyed;
const maxPayload=this._maxPayload,maxFrags=this._maxFragments,fast=this.fast;
const fragErr=maxFrags>0?'too many fragments':'fragmented message exceeded maxPayload';
let msgIn=0,bytesIn=0;
while(L-o>=2){
const b0=buf[o],b1=buf[o+1],op=b0&15;
if(b1<126){
if(maskReq){this._fire(this.error,new Error('unmasked client frame'));this._onClose(1002);return;}
const t=2+b1;if(L-o<t)break;o+=t;
if(op===1||op===2){if(b0&128){this._emitRecv(b1?buf.subarray(o-b1,o):EMPTY,op);msgIn++;bytesIn+=b1;}else{this._fragOp=op;this._frags=[b1?buf.subarray(o-b1,o):EMPTY];this._fragBytes=b1;this._fragCount=1;}}
else if(op===0&&this._frags){
this._fragCount++;this._fragBytes+=b1;
if((maxFrags>0&&this._fragCount>maxFrags)||(maxPayload>0&&this._fragBytes>maxPayload)){this._frags=null;this._fire(this.error,new Error(fragErr));this._onClose(1009);return;}
this._frags.push(b1?buf.subarray(o-b1,o):EMPTY);
if(b0&128){combineFin(this);msgIn++;bytesIn+=this._fragBytes;}}
else if(op===9){const p=b1?buf.subarray(o-b1,o):EMPTY;if(autoPong&&alive)sendPong(sock,isSrv,p,b1>0,fast);dispatchPing(this,p);}
else if(op===10)this._onPong();
else if(op===8){this._onClose(b1>=2?(buf[o-b1]<<8)|buf[o-b1+1]:1005);return;}
continue;}
const masked=b1&128;
if(isSrv&&!masked&&maskReq){this._fire(this.error,new Error('unmasked client frame'));this._onClose(1002);return;}
if(!isSrv&&masked&&maskReq){this._fire(this.error,new Error('masked server frame'));this._onClose(1002);return;}
let plen=b1&127,hs=2;
if(plen===126){if(L-o<4)break;plen=(buf[o+2]<<8)|buf[o+3];hs=4;}
else if(plen===127){if(L-o<10)break;if(buf.readUInt32BE(o+2)!==0){this._fire(this.error,new Error('payload exceeded maxPayload'));this._onClose(1009);return;}plen=buf.readUInt32BE(o+6);hs=10;}
if(maxPayload>0&&plen>maxPayload){this._fire(this.error,new Error('payload exceeded maxPayload'));this._onClose(1009);return;}
if(masked)hs+=4;
const tot=hs+plen;if(L-o<tot)break;
const p=plen?buf.subarray(o+hs,o+tot):EMPTY;
if(masked&&plen)mask32(p,0,plen,buf.readInt32LE(o+hs-4));
o+=tot;
if(op===1||op===2){if(b0&128){this._emitRecv(p,op);msgIn++;bytesIn+=plen;}else{this._fragOp=op;this._frags=[p];this._fragBytes=plen;this._fragCount=1;}}
else if(op===0&&this._frags){
this._fragCount++;this._fragBytes+=plen;
if((maxFrags>0&&this._fragCount>maxFrags)||(maxPayload>0&&this._fragBytes>maxPayload)){this._frags=null;this._fire(this.error,new Error(fragErr));this._onClose(1009);return;}
this._frags.push(p);
if(b0&128){combineFin(this);msgIn++;bytesIn+=this._fragBytes;}}
else if(op===9){if(autoPong&&alive)sendPong(sock,isSrv,p,plen>0,fast);dispatchPing(this,p);}
else if(op===10)this._onPong();
else if(op===8){this._onClose(plen>=2?(p[0]<<8)|p[1]:1005);return;}}
if(this._rbLen>0){
if(o<L){this._rbOff+=o;this._rbLen=L-o;if(this._rbOff>(this._rb.length>>1)){this._rb.copy(this._rb,0,this._rbOff,this._rbOff+this._rbLen);this._rbOff=0;}}
else{this._rbLen=0;this._rbOff=0;if(this._rb.length>(this._rbInitSize<<2))this._rb=Buffer.allocUnsafe(this._rbInitSize);}}
else if(o<L){const r=L-o;
if(r>this._rb.length){this._rb=Buffer.allocUnsafe(Math.max(this._rb.length<<1,r));c.copy(this._rb,0,o,L);}
else if(c.buffer===this._rb.buffer){if(o!==0)this._rb.copyWithin(0,o,L);}
else c.copy(this._rb,0,o,L);
this._rbLen=r;this._rbOff=0;}
if(this._statsEnabled){this._msgIn+=msgIn;this._bytesIn+=bytesIn;}}
export function _parseHandshake(chunk){
this._pushRb(chunk);
const avail=this._rb.subarray(this._rbOff,this._rbOff+this._rbLen),idx=avail.indexOf(CRLF);
if(idx===-1)return;
const hdr=avail.toString('latin1',0,idx),tailStart=idx+4,tailLen=this._rbLen-tailStart;
if(!hdr.includes('101')){this._fire(this.error,new Error('upgrade failed'));this._onClose(1006);return;}
if(this.opts.skipAcceptCheck!==true){
const lines=hdr.split('\r\n');let accept='';
for(let i=1;i<lines.length;i++){const l=lines[i];if(!l)continue;const ci=l.indexOf(':');
if(ci!==-1&&l.slice(0,ci).trim().toLowerCase()==='sec-websocket-accept'){accept=l.slice(ci+1).trim();break;}}
if(accept!==this._expect){this._fire(this.error,new Error('bad accept'));this._onClose(1006);return;}}
this.isOpen=true;this.isClosed=false;this.reconnectAttempts=0;this._connectedAt=Date.now();
this._setState('connected');this._healthFailures=0;this._qualityScore=100;
if(this._reconnectTimer){clearTimeout(this._reconnectTimer);this._reconnectTimer=null;}
if(this._timer){clearTimeout(this._timer);this._timer=null;}
this._startPing();
if(this._queueEnabled)this._flushQueue();
this._fire(this.online);
const tail=tailLen>0?Buffer.from(avail.subarray(tailStart,tailStart+tailLen)):null;
this._rbLen=0;this._rbOff=0;
if(tail)this._onData(tail);}
