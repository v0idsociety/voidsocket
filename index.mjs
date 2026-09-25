// xherz
import VoidSocket from './socket.mjs';
import VoidServer from './server.mjs';
import { pack, packInto, frameLen, packHeader, mask32, fastMask32, parseFrames, returnHeaderBuf, getFrameBuf, releaseFrameBuf, ERR_CODES } from './shared.mjs';
export { pack, packInto, frameLen, packHeader, mask32, fastMask32, parseFrames, returnHeaderBuf, getFrameBuf, releaseFrameBuf, ERR_CODES } from './shared.mjs';
export { VoidSocket, VoidServer };
export { VoidSocket as Void, VoidSocket as v0id, VoidSocket as vd };
export { VoidServer as VoidSocketServer, VoidServer as v0idServer, VoidServer as vdServer, VoidServer as Server };
VoidSocket.Server=VoidSocket.VoidServer=VoidSocket.VoidSocketServer=VoidSocket.v0idServer=VoidSocket.vdServer=VoidServer;
VoidSocket.pack=pack;VoidSocket.packInto=packInto;VoidSocket.frameLen=frameLen;VoidSocket.packHeader=packHeader;VoidSocket.mask32=mask32;VoidSocket.fastMask32=fastMask32;VoidSocket.parseFrames=parseFrames;VoidSocket.returnHeaderBuf=returnHeaderBuf;VoidSocket.getFrameBuf=getFrameBuf;VoidSocket.releaseFrameBuf=releaseFrameBuf;
export default VoidSocket;
