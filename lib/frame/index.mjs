import { EMPTY } from '../constants/index.mjs';
import { fastMask32, nextMask, mask32 } from '../masking/index.mjs';
import { getHeaderBuf } from '../buffer-pool/index.mjs';
function fillFrame(f,op,data,mask,fast,len,hs,isStr,isBuf){
f[0]=0x80|op;
if(len<=125)f[1]=(mask?0x80:0)|len;
else if(len<=65535){f[1]=(mask?0x80:0)|126;f[2]=len>>8;f[3]=len&255;}
else{f[1]=(mask?0x80:0)|127;f[2]=f[3]=f[4]=f[5]=0;f[6]=(len>>>24)&255;f[7]=(len>>16)&255;f[8]=(len>>8)&255;f[9]=len&255;}
if(mask){const mo=hs-4;f.writeInt32LE(fastMask32(),mo);if(len>0){if(isStr)f.write(data,hs);else if(isBuf)f.set(data,hs);else if(data)f.set(Buffer.from(data.buffer||data,data.byteOffset||0,len),hs);mask32(f,hs,len,f.readInt32LE(mo));}}
else if(len>0){if(isStr)f.write(data,hs);else if(isBuf)f.set(data,hs);else if(data)f.set(Buffer.from(data.buffer||data,data.byteOffset||0,len),hs);}
return hs+len;}
export function pack(op,data,mask=true,fast=true){
const isStr=typeof data==='string',isBuf=!isStr&&Buffer.isBuffer(data);
const len=isStr?Buffer.byteLength(data):(isBuf?data.length:(data?data.byteLength||0:0));
const hs=(len<=125?2:len<=65535?4:10)+(mask?4:0);
const f=Buffer.allocUnsafe(hs+len);
fillFrame(f,op,data,mask,fast,len,hs,isStr,isBuf);
return f;}
export function packInto(buf,op,data,mask=true,fast=true){
const isStr=typeof data==='string',isBuf=!isStr&&Buffer.isBuffer(data);
const len=isStr?Buffer.byteLength(data):(isBuf?data.length:(data?data.byteLength||0:0));
const hs=(len<=125?2:len<=65535?4:10)+(mask?4:0);
return fillFrame(buf,op,data,mask,fast,len,hs,isStr,isBuf);}
export function frameLen(data,mask=true){
const isStr=typeof data==='string',isBuf=!isStr&&Buffer.isBuffer(data);
const len=isStr?Buffer.byteLength(data):(isBuf?data.length:(data?data.byteLength||0:0));
return(len<=125?2:len<=65535?4:10)+(mask?4:0)+len;}
export function packHeader(op,len,mask=true,fast=true){
const hs=(len<=125?2:len<=65535?4:10)+(mask?4:0);
let hdr=getHeaderBuf(hs);
if(!hdr)hdr=Buffer.allocUnsafe(hs);
hdr[0]=0x80|op;
if(len<=125)hdr[1]=(mask?0x80:0)|len;
else if(len<=65535){hdr[1]=(mask?0x80:0)|126;hdr[2]=len>>8;hdr[3]=len&0xFF;}
else{hdr[1]=(mask?0x80:0)|127;hdr[2]=0;hdr[3]=0;hdr[4]=0;hdr[5]=0;hdr[6]=(len>>>24)&0xFF;hdr[7]=(len>>16)&0xFF;hdr[8]=(len>>8)&0xFF;hdr[9]=len&0xFF;}
let m32=0;
if(mask){const mo=hs-4;if(fast){m32=fastMask32();hdr.writeInt32LE(m32,mo);}else m32=nextMask(hdr,mo);}
return{hdr,m32,hs};}
export function parseFrames(b,onFrame){
let o=0,l=b.length;
while(l-o>=2){
const b1=b[o+1];
if(b1<126){const t=2+b1;if(l-o<t)break;const b0=b[o];o+=t;onFrame(b1?b.subarray(o-b1,o):EMPTY,b0&15,(b0&128)!==0);continue;}
let p=b1&127,hs=2;
if(p===126){if(l-o<4)break;p=(b[o+2]<<8)|b[o+3];hs=4;}
else if(p===127){if(l-o<10)break;p=b.readUInt32BE(o+6);hs=10;}
if(b1&128){if(l-o<hs+4)break;hs+=4;}
const t=hs+p;
if(l-o<t)break;
const b0=b[o],pl=p?b.subarray(o+hs,o+t):EMPTY;
if(b1&128)mask32(pl,0,p,b.readInt32LE(o+hs-4));
o+=t;onFrame(pl,b0&15,(b0&128)!==0);}
return o;}
