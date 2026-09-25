export function _rlCheck(data,cb){if(!this._rlEnabled)return true;if(this._rlTokens>0){this._rlTokens--;return true;}this._rlQueue.push({data,cb});return false;}
export function _drainRlQueue(){while(this._rlQueue.length>0&&this._rlTokens>0){const it=this._rlQueue.shift();this._rlTokens--;const cb=it.cb;it.cb=null;this._sendDirect(it.data,cb);}}
export function _nextUrl(){if(!this._urls||this._urls.length<=1)return;this._urlIndex=(this._urlIndex+1)%this._urls.length;this.url=this._urls[this._urlIndex];}
