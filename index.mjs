// xherz
import VoidSocket from './socket.mjs';
import VoidServer from './server.mjs';
import { pack, packHeader, mask32, fastMask32, parseFrames, returnHeaderBuf, ERR_CODES } from './shared.mjs';

export { pack, packHeader, mask32, fastMask32, parseFrames, returnHeaderBuf, ERR_CODES } from './shared.mjs';
export { VoidSocket, VoidServer };
export { VoidSocket as Void, VoidSocket as v0id, VoidSocket as vd };
export { VoidServer as VoidSocketServer, VoidServer as v0idServer, VoidServer as vdServer, VoidServer as Server };

VoidSocket.Server = VoidServer;
VoidSocket.VoidServer = VoidServer;
VoidSocket.VoidSocketServer = VoidServer;
VoidSocket.v0idServer = VoidServer;
VoidSocket.vdServer = VoidServer;
VoidSocket.pack = pack;
VoidSocket.packHeader = packHeader;
VoidSocket.mask32 = mask32;
VoidSocket.fastMask32 = fastMask32;
VoidSocket.parseFrames = parseFrames;
VoidSocket.returnHeaderBuf = returnHeaderBuf;

export default VoidSocket;
