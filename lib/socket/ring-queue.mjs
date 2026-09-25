class RingQueue{
constructor(initialCap=256){let cap=Math.max(1,initialCap);if(cap<256)cap=256;let power=1;while(power<cap)power<<=1;this._buf=new Array(power);this._head=0;this._tail=0;this._size=0;}
get length(){return this._size;}
push(v){if(this._size===this._buf.length){const nb=new Array(this._buf.length*2);for(let i=0;i<this._size;i++)nb[i]=this._buf[(this._head+i)&(this._buf.length-1)];this._head=0;this._tail=this._size;this._buf=nb;}this._buf[this._tail]=v;this._tail=(this._tail+1)&(this._buf.length-1);this._size++;}
shift(){if(this._size===0)return undefined;const v=this._buf[this._head];this._buf[this._head]=undefined;this._head=(this._head+1)&(this._buf.length-1);this._size--;return v;}
clear(){this._head=0;this._tail=0;this._size=0;this._buf.fill(undefined);}}
export default RingQueue;
export { RingQueue };
