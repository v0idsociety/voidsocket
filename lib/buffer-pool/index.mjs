const FRAME_POOL_SIZE=64;
const _framePool2=[],_framePool4=[],_framePool10=[];
export { FRAME_POOL_SIZE, _framePool2, _framePool4, _framePool10 };
for(let i=0;i<FRAME_POOL_SIZE;i++){_framePool2.push(Buffer.allocUnsafe(6));_framePool4.push(Buffer.allocUnsafe(8));_framePool10.push(Buffer.allocUnsafe(14));}
export function getHeaderBuf(hs){
if(hs===6&&_framePool2.length)return _framePool2.pop();
if(hs===8&&_framePool4.length)return _framePool4.pop();
if(hs===14&&_framePool10.length)return _framePool10.pop();
return null;}
export function returnHeaderBuf(buf){
const l=buf.length;
if(l===6&&_framePool2.length<FRAME_POOL_SIZE)_framePool2.push(buf);
else if(l===8&&_framePool4.length<FRAME_POOL_SIZE)_framePool4.push(buf);
else if(l===14&&_framePool10.length<FRAME_POOL_SIZE)_framePool10.push(buf);}
const FRAG_POOL_SIZE=128;
const _fragPool=[];
export { FRAG_POOL_SIZE, _fragPool };
for(let i=0;i<FRAG_POOL_SIZE;i++)_fragPool.push({buf:Buffer.allocUnsafe(16384),len:0});
export function getFragBuf(){return _fragPool.pop()||{buf:Buffer.allocUnsafe(16384),len:0};}
export function returnFragBuf(f){f.len=0;if(_fragPool.length<FRAG_POOL_SIZE)_fragPool.push(f);}
const FRAME_CLASSES=[{size:256,cap:256,stack:[]},{size:2048,cap:128,stack:[]},{size:16384,cap:32,stack:[]},{size:131072,cap:8,stack:[]}];
const NO_POOL=typeof process!=='undefined'&&process.env&&process.env.VOIDSKT_NO_POOL==='1';
const POOL_MIN=+(typeof process!=='undefined'&&process.env&&process.env.VOIDSKT_POOL_MIN||4096)||4096;
export function usePool(total){return !NO_POOL&&total>POOL_MIN;}
export function getFrameBuf(total){
if(!NO_POOL)for(let i=0;i<FRAME_CLASSES.length;i++){const c=FRAME_CLASSES[i];if(total<=c.size)return c.stack.pop()||Buffer.allocUnsafe(c.size);}
return Buffer.allocUnsafe(total);}
export function releaseFrameBuf(buf){
if(NO_POOL)return;
for(let i=0;i<FRAME_CLASSES.length;i++){const c=FRAME_CLASSES[i];if(buf.length===c.size){if(c.stack.length<c.cap)c.stack.push(buf);return;}}}
