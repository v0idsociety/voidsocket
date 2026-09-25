import { createHash } from 'node:crypto';
import { GUID } from '../constants/index.mjs';
export function makeCloseFrame(code=1000,mask=false){
const CLOSE_FRAME=Buffer.from([0x88,0x82,0x00,0x00,0x00,0x00,0x03,0xE8]);
const CLOSE_FRAME_UNMASKED=Buffer.from([0x88,0x02,0x03,0xE8]);
if(code<=0||code===1005)return mask?Buffer.from([0x88,0x80,0x00,0x00,0x00,0x00]):Buffer.from([0x88,0x00]);
if(code===1000)return mask?CLOSE_FRAME:CLOSE_FRAME_UNMASKED;
const b=Buffer.allocUnsafe(mask?8:4);
b[0]=0x88;b[1]=(mask?0x80:0)|2;
if(mask){b[2]=0;b[3]=0;b[4]=0;b[5]=0;b[6]=(code>>8)&0xFF;b[7]=code&0xFF;}
else{b[2]=(code>>8)&0xFF;b[3]=code&0xFF;}
return b;}
export function abortHandshake(socket,code,message,headers={}){
if(socket.writable){
let res=`HTTP/1.1 ${code} ${message}\r\nConnection: close\r\nContent-Type: text/plain\r\nContent-Length: ${Buffer.byteLength(message)}\r\n`;
for(const[k,v]of Object.entries(headers))res+=`${k}: ${v}\r\n`;
res+=`\r\n${message}`;
socket.end(res,()=>{socket.destroy();});
}else socket.destroy();}
export function computeAcceptKey(key){return createHash('sha1').update(key+GUID).digest('base64');}
