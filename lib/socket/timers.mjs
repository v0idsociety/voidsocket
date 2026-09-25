export function setAutoTimeout(fn,delay){const id=++this._msgId;const timer=globalThis.setTimeout(()=>{this._autoTimeouts.delete(id);fn();},delay);this._autoTimeouts.set(id,timer);return id;}
export function clearAutoTimeout(id){const t=this._autoTimeouts.get(id);if(t){globalThis.clearTimeout(t);this._autoTimeouts.delete(id);}}
export function setTimeout(fn,delay){return this.setAutoTimeout(fn,delay);}
export function clearTimeout(id){return this.clearAutoTimeout(id);}
export function clearAllTimeouts(){for(const[,t]of this._autoTimeouts)globalThis.clearTimeout(t);this._autoTimeouts.clear();}
