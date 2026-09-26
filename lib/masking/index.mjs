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
let o=buf.byteOffset+off,pOff=off,pLen=len;
const rem=(8-(o&7))&7;
if(rem>0){
const prefixLen=rem<len?rem:len;
for(let i=0;i<prefixLen;i++)buf[pOff+i]^=(m32>>>((i&3)<<3))&255;
if(rem>=len)return;
pOff+=rem;pLen-=rem;o+=rem;}
const shift=(rem&3)<<3;
const curM32=((m32>>>shift)|(m32<<(32-shift)))>>>0;
const curM32n=BigInt(curM32);
const m64=curM32n|(curM32n<<32n);
const words=pLen>>>3;
const u=new BigUint64Array(buf.buffer,o,words);
const e8=words&~7;let i=0;
for(;i<e8;i+=8){u[i]^=m64;u[i+1]^=m64;u[i+2]^=m64;u[i+3]^=m64;u[i+4]^=m64;u[i+5]^=m64;u[i+6]^=m64;u[i+7]^=m64;}
for(;i<words;i++)u[i]^=m64;
const trailing=pLen&7;
if(trailing>0){
const start=len-trailing;
for(let k=start;k<len;k++)buf[off+k]^=(m32>>>((k&3)<<3))&255;}}

