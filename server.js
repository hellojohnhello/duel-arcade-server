/* ============================================================
   Duel Arcade — relay server
   Matchmaking by 4-letter room code + message relay between
   exactly two players. Works on any network (no peer-to-peer).
   ============================================================ */

const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const ROOM_TTL_MS = 1000 * 60 * 30;        // forget empty rooms after 30 min
const SELF_PING_MS = 1000 * 60 * 13;       // self-ping under the 15-min idle limit

// rooms: code -> { host: ws|null, guest: ws|null, createdAt }
const rooms = new Map();

function log(...a){ console.log(new Date().toISOString(), ...a); }

// ---- HTTP server (also serves healthcheck for keep-alive pings) ----
const server = http.createServer((req, res) => {
  if (req.url === '/healthcheck' || req.url === '/' ) {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('ok ' + rooms.size + ' rooms');
    return;
  }
  res.writeHead(404); res.end('not found');
});

const wss = new WebSocketServer({ server });

function send(ws, obj){
  if (ws && ws.readyState === ws.OPEN) {
    try { ws.send(JSON.stringify(obj)); } catch (e) {}
  }
}
function otherIn(room, ws){
  if (!room) return null;
  return room.host === ws ? room.guest : room.host;
}
function cleanupRoom(code){
  const room = rooms.get(code);
  if (!room) return;
  const empty = (!room.host || room.host.readyState !== room.host.OPEN) &&
                (!room.guest || room.guest.readyState !== room.guest.OPEN);
  if (empty) { rooms.delete(code); log('room closed', code); }
}

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.role = null;   // 'host' | 'guest'
  ws.code = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch (e) { return; }

    // ---- create a room (host) ----
    if (msg.t === 'host') {
      // generate a unique code
      let code = (msg.code || '').toUpperCase();
      if (!code || rooms.has(code)) {
        const AB = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
        do { code = Array.from({length:4}, () => AB[Math.floor(Math.random()*AB.length)]).join(''); }
        while (rooms.has(code));
      }
      rooms.set(code, { host: ws, guest: null, createdAt: Date.now() });
      ws.role = 'host'; ws.code = code;
      send(ws, { t: 'hosted', code });
      log('room created', code);
      return;
    }

    // ---- join a room (guest) ----
    if (msg.t === 'join') {
      const code = (msg.code || '').toUpperCase();
      const room = rooms.get(code);
      if (!room) { send(ws, { t: 'join_error', reason: 'no_room', code }); return; }
      if (room.guest && room.guest.readyState === room.guest.OPEN) {
        send(ws, { t: 'join_error', reason: 'full', code }); return;
      }
      room.guest = ws; ws.role = 'guest'; ws.code = code;
      send(ws, { t: 'joined', code });
      // tell both sides the peer is present
      send(room.host, { t: 'peer_joined' });
      send(ws, { t: 'peer_joined' });
      log('room joined', code);
      return;
    }

    // ---- relay any game payload to the other player ----
    if (msg.t === 'relay') {
      const room = rooms.get(ws.code);
      const peer = otherIn(room, ws);
      if (peer) send(peer, { t: 'relay', from: ws.role, payload: msg.payload });
      return;
    }
  });

  ws.on('close', () => {
    const room = rooms.get(ws.code);
    if (room) {
      const peer = otherIn(room, ws);
      if (peer) send(peer, { t: 'peer_left' });
      if (room.host === ws) room.host = null;
      if (room.guest === ws) room.guest = null;
      cleanupRoom(ws.code);
    }
  });

  ws.on('error', () => {});
});

// ---- ping clients periodically; drop dead sockets ----
const heartbeat = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) { try { ws.terminate(); } catch(e){} return; }
    ws.isAlive = false;
    try { ws.ping(); } catch(e){}
  });
}, 30000);

// ---- forget stale empty rooms ----
const sweeper = setInterval(() => {
  const cutoff = Date.now() - ROOM_TTL_MS;
  for (const [code, room] of rooms) {
    const empty = (!room.host || room.host.readyState !== 1) &&
                  (!room.guest || room.guest.readyState !== 1);
    if (empty && room.createdAt < cutoff) { rooms.delete(code); log('room expired', code); }
  }
}, 1000 * 60 * 5);

// ---- backup self-ping to reduce idle spin-down (best-effort) ----
let selfPingTimer = null;
function startSelfPing(){
  const url = process.env.SELF_URL; // set this in Render to your service URL for backup keep-alive
  if (!url) return;
  selfPingTimer = setInterval(() => {
    const mod = url.startsWith('https') ? require('https') : require('http');
    try { mod.get(url + '/healthcheck', (r) => { r.resume(); }).on('error', () => {}); } catch(e){}
  }, SELF_PING_MS);
}

server.listen(PORT, () => { log('Duel Arcade relay listening on', PORT); startSelfPing(); });

wss.on('close', () => { clearInterval(heartbeat); clearInterval(sweeper); if (selfPingTimer) clearInterval(selfPingTimer); });
