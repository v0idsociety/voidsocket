// xherz
import { createHttpServer, createHttpsServer, abortHandshake, computeAcceptKey, pack, packInto, frameLen, getFrameBuf, releaseFrameBuf, usePool } from './shared.mjs';
import VoidSocket from './socket.mjs';
class VoidServer{
constructor(opts={},cb){
if(typeof opts==='function'){cb=opts;opts={};}
this.opts=opts;this.clientOpts=opts.clientOpts?{...opts.clientOpts}:{};
for(const k of['maxPayload','autoPong','autoPing','pingInterval','pingTimeout','bufferSize','json','compress','compressThreshold','compressLevel'])if(opts[k]!==undefined)this.clientOpts[k]=opts[k];
this.path=opts.path?(opts.path.startsWith('/')?opts.path:'/'+opts.path):null;
this.verifyClient=opts.verifyClient||null;this.selectProtocol=opts.selectProtocol||null;
this.maxConnections=opts.maxConnections??Infinity;
this.clientTracking=opts.clientTracking!==false;this.clients=this.clientTracking?new Set():null;
this.headers=opts.headers||null;
this.connection=null;this.listening=null;this.onClose=null;this.error=null;this.onHeaders=null;
if(cb)this.connection=cb;
this.server=null;this.isListening=false;this._isSelfServer=false;
if(opts.server){this.server=opts.server;this._attachServer(this.server);}
else if(opts.port!=null){
if(typeof opts.port!=='number'||opts.port<0||opts.port>65535)throw new Error('Invalid port number');
this._isSelfServer=true;
const tlsOpts=opts.tls||opts.https;
this.server=tlsOpts?createHttpsServer(tlsOpts):createHttpServer();
this.server.on('request',(req,res)=>{try{res.writeHead(426,{'Content-Length':'0','Connection':'close'});res.end();}catch{}});
this._attachServer(this.server);
this.server.listen(opts.port,opts.host,opts.backlog,()=>{this.isListening=true;this._fire(this.listening);this.emit('listening');});}}
_attachServer(server){
server.on('upgrade',(req,socket,head)=>{
try{
if(this.shouldHandle(req))this.handleUpgrade(req,socket,head,()=>{});
else if(this._isSelfServer)abortHandshake(socket,404,'Not Found');
}catch(e){this._fire(this.error,e);this.emit('error',e);if(!socket.destroyed)socket.destroy();}});
server.on('error',err=>{this._fire(this.error,err);this.emit('error',err);});
server.on('close',()=>{this.isListening=false;this._fire(this.onClose);this.emit('close');});
if(server.listening)this.isListening=true;
else server.once('listening',()=>{this.isListening=true;this._fire(this.listening);this.emit('listening');});}
shouldHandle(req){
if(!this.path)return true;
const url=req.url||'/',q=url.indexOf('?');
return(q===-1?url:url.slice(0,q))===this.path;}
handleUpgrade(req,socket,head,cb){
const bad=(code,msg,extra)=>{abortHandshake(socket,code,msg,extra);};
try{
if(req.method!=='GET'){bad(405,'Method Not Allowed');return;}
const h=req.headers,up=h.upgrade;
if(!up||up.toLowerCase()!=='websocket'){bad(400,'Bad Request');return;}
const conn=h.connection;
if(!conn||typeof conn!=='string'||!conn.toLowerCase().split(/,\s*/).includes('upgrade')){bad(400,'Bad Request');return;}
if(h['sec-websocket-version']!=='13'){bad(426,'Upgrade Required',{'Sec-WebSocket-Version':'13'});return;}
const key=h['sec-websocket-key'];
if(!key){bad(400,'Bad Request');return;}
if(this.clients&&this.clients.size>=this.maxConnections){bad(503,'Service Unavailable');return;}
if(this.verifyClient){
const info={origin:h.origin,secure:!!(req.socket.authorized||req.socket.encrypted),req};
if(this.verifyClient.length===2){this.verifyClient(info,(ok,code,msg,xh)=>{if(!ok){bad(code||401,msg||'Unauthorized',xh);return;}this._completeUpgrade(key,req,socket,head,cb);});return;}
const r=this.verifyClient(info);
if(r&&typeof r.then==='function'){r.then(ok=>{if(!ok){bad(401,'Unauthorized');return;}this._completeUpgrade(key,req,socket,head,cb);}).catch(e=>{this._fire(this.error,e);this.emit('error',e);bad(500,'Internal Server Error');});return;}
if(!r){bad(401,'Unauthorized');return;}}
this._completeUpgrade(key,req,socket,head,cb);
}catch(e){this._fire(this.error,e);this.emit('error',e);if(!socket.destroyed)socket.destroy();}}
_completeUpgrade(key,req,socket,head,cb){
if(socket.destroyed)return;
let proto=null;
const subs=req.headers['sec-websocket-protocol'];
if(subs&&typeof subs==='string'&&this.selectProtocol)proto=this.selectProtocol(subs.split(/,\s*/),req);
const rh=['HTTP/1.1 101 Switching Protocols','Upgrade: websocket','Connection: Upgrade',`Sec-WebSocket-Accept: ${computeAcceptKey(key)}`];
if(proto)rh.push(`Sec-WebSocket-Protocol: ${proto}`);
if(this.headers){const x=typeof this.headers==='function'?this.headers(req):this.headers;if(x)for(const[k,v]of Object.entries(x))rh.push(`${k}: ${v}`);}
if(this.onHeaders)this._fire(this.onHeaders,rh,req);
socket.write(rh.join('\r\n')+'\r\n\r\n');
const client=new VoidSocket(null,{...this.clientOpts,socket,isServer:true,req,head});
if(this.clientTracking&&this.clients){this.clients.add(client);const cl=()=>{this.clients.delete(client);client.off('offline',cl);};client.on('offline',cl);}
if(cb)cb(client,req);
this.emit('connection',client,req);}
broadcast(data,filter){
if(!this.clients||this.clients.size===0)return 0;
const str=typeof data==='string';
const obj=!str&&!Buffer.isBuffer(data)&&typeof data==='object'&&data!==null&&!ArrayBuffer.isView(data);
const payload=obj?JSON.stringify(data):data;
const total=frameLen(payload,false);
if(!usePool(total)){
const frame=pack(str||obj?1:2,payload,false);
let n=0;
for(const c of this.clients)if(c.isOpen&&(!filter||filter(c))){c.raw(frame);n++;}
return n;}
const fb=getFrameBuf(total);
packInto(fb,str||obj?1:2,payload,false);
const view=fb.subarray(0,total);
let n=0,pending=0;
const done=()=>{if(--pending===0)releaseFrameBuf(fb);};
for(const c of this.clients){
if(!c.isOpen||(filter&&!filter(c)))continue;
pending++;
try{c.raw(view,done);}catch(e){done();throw e;}
n++;}
if(pending===0)releaseFrameBuf(fb);
return n;}
address(){return this.server?this.server.address():null;}
close(cb){
if(this.clients){for(const c of this.clients){c.close(1001);if(c._sock&&!c._sock.destroyed)c._sock.destroy();}this.clients.clear();}
if(this.server&&this._isSelfServer){
if(typeof this.server.closeAllConnections==='function')this.server.closeAllConnections();
this.server.close(err=>{this.isListening=false;this._fire(this.onClose);this.emit('close');if(cb)cb(err);});}
else{this.isListening=false;this._fire(this.onClose);this.emit('close');if(cb)cb();}}
_fire(h,a,b){if(h==null)return;if(typeof h==='function')return h(a,b);for(let i=0;i<h.length;i++)h[i](a,b);}
_evKey(ev){switch(ev){
case 'connection':return 'connection';
case 'listening':return 'listening';
case 'close':case 'offline':return 'onClose';
case 'error':return 'error';
case 'headers':return 'onHeaders';
default:return '';}}
on(ev,fn){const k=this._evKey(ev);if(!k)return this;const cur=this[k];if(cur==null)this[k]=fn;else if(typeof cur==='function')this[k]=[cur,fn];else cur.push(fn);return this;}
off(ev,fn){const k=this._evKey(ev);if(!k)return this;const cur=this[k];if(cur===fn)this[k]=null;else if(Array.isArray(cur)){const i=cur.indexOf(fn);if(i!==-1){if(cur.length===2)this[k]=cur[1-i];else cur.splice(i,1);}}return this;}
once(ev,fn){const w=(a,b)=>{this.off(ev,w);fn(a,b);};return this.on(ev,w);}
emit(ev,a,b){const k=this._evKey(ev);if(k)this._fire(this[k],a,b);}}
export default VoidServer;
export { VoidServer };
