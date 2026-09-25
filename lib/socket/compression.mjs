import { deflateRawSync } from 'node:zlib';
export function enableCompression(threshold=this._compressThreshold,level=this._compressLevel){this._compressEnabled=true;this._compressThreshold=threshold;this._compressLevel=level;return this;}
export function disableCompression(){this._compressEnabled=false;return this;}
export function isCompressionEnabled(){return this._compressEnabled;}
export function getCompressionStats(){return{compressionEnabled:this._compressEnabled,compressThreshold:this._compressThreshold,compressLevel:this._compressLevel,compressSent:this._compressSent,compressReceived:this._compressReceived};}
export function _compressPayload(data){
if(!this._compressEnabled||data.length<this._compressThreshold)return data;
try{const c=deflateRawSync(data,{level:this._compressLevel});
if(c.length+1<data.length){this._compressSent++;const b=Buffer.allocUnsafe(c.length+1);b[0]=1;b.set(c,1);return b;}}catch{return data;}
return data;}
