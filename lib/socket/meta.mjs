export function setMeta(key,value){if(typeof key==='object')Object.assign(this._meta,key);else this._meta[key]=value;return this;}
export function getMeta(key){return key?this._meta[key]:this._meta;}
export function hasMeta(key,value){if(value!==undefined)return this._meta[key]===value;return key in this._meta;}
export function removeMeta(key){delete this._meta[key];return this;}
export function _setState(s){if(this._state===s)return;const old=this._state;this._state=s;this._fire(this.onStateChange,s,old);}
export function setName(name){this._name=name;return this;}
