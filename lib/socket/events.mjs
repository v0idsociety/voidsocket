export function _throwError(code,message){const e=new Error(`[VOIDSOCKET:${code}] ${message}`);e.code=code;this._fire(this.error,e);return e;}
export function _fire(h,a,b){if(h==null)return;if(typeof h==='function')return h(a,b);for(let i=0;i<h.length;i++)h[i](a,b);}
export function _evKey(ev){switch(ev){
case 'message':case 'recv':return 'recv';
case 'open':case 'online':return 'online';
case 'close':case 'offline':return 'offline';
case 'error':return 'error';
case 'ping':return 'onPing';
case 'flushed':case 'drain':return 'flushed';
case 'reconnecting':return 'reconnecting';
case 'connecting':return 'connecting';
case 'statechange':case 'state':return 'onStateChange';
default:return '';}}
export function on(ev,fn){const key=this._evKey(ev);if(!key)return this;const cur=this[key];
if(cur==null)this[key]=fn;else if(typeof cur==='function')this[key]=[cur,fn];else cur.push(fn);return this;}
export function off(ev,fn){const key=this._evKey(ev);if(!key)return this;const cur=this[key];
if(cur===fn)this[key]=null;
else if(Array.isArray(cur)){const i=cur.indexOf(fn);if(i!==-1){if(cur.length===2)this[key]=cur[1-i];else cur.splice(i,1);}}return this;}
export function once(ev,fn){const wrapped=(a,b)=>{this.off(ev,wrapped);fn(a,b);};return this.on(ev,wrapped);}
