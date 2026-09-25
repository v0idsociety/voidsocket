import { randomFillSync } from 'node:crypto';
const POOL_SIZE=4096;
export { POOL_SIZE };
export const pool=Buffer.allocUnsafe(POOL_SIZE);
export let poolOff=POOL_SIZE;
let seed=(Date.now()^(Math.random()*0x7FFFFFFF))|0;
export { seed };
export function fastMask32(){seed^=seed<<13;seed^=seed>>>17;seed^=seed<<5;return seed|0;}
export function nextMask(buf,off){
if(poolOff+4>POOL_SIZE){randomFillSync(pool);poolOff=0;}
const m0=pool[poolOff++],m1=pool[poolOff++],m2=pool[poolOff++],m3=pool[poolOff++];
buf[off]=m0;buf[off+1]=m1;buf[off+2]=m2;buf[off+3]=m3;
return(m0|(m1<<8)|(m2<<16)|(m3<<24));}
export function mask32(buf,off,len,m32){
if(len<=64){
const m0=m32&255,m1=(m32>>>8)&255,m2=(m32>>>16)&255,m3=(m32>>>24)&255;
const e=len&~3;let i=0;
for(;i<e;i+=4){buf[off+i]^=m0;buf[off+i+1]^=m1;buf[off+i+2]^=m2;buf[off+i+3]^=m3;}
for(;i<len;i++)buf[off+i]^=(m32>>>((i&3)<<3))&255;
return;}
const o=buf.byteOffset+off;
if((o&3)===0){
const u=new Uint32Array(buf.buffer,o,len>>>2),e4=u.length&~3;
for(let i=0;i<e4;i+=4){u[i]^=m32;u[i+1]^=m32;u[i+2]^=m32;u[i+3]^=m32;}
for(let i=e4;i<u.length;i++)u[i]^=m32;
const rem=len&3;
if(rem!==0){const b=off+(len&~3);buf[b]^=m32&255;if(rem>1)buf[b+1]^=(m32>>>8)&255;if(rem>2)buf[b+2]^=(m32>>>16)&255;}
}else{
const dv=new DataView(buf.buffer,o,len),e4=len&~3;
for(let i=0;i<e4;i+=4)dv.setUint32(i,dv.getUint32(i,1)^m32,1);
for(let i=e4;i<len;i++)buf[off+i]^=(m32>>>((i&3)<<3))&255;}}
