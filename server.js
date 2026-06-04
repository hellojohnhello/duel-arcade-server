/* ============================================================
   Duel Arcade — relay server (v3)
   - Matchmaking by 4-letter room code.
   - Host is the authority; relays state to all guests.
   - Guests send inputs to the host.
   - NEW in v3:
       * Players can rejoin a room by code at any time (dedupe by pid).
       * Host migration: if the host leaves, the oldest remaining
         player is promoted to host so the room stays alive.
       * Empty rooms are kept for a short grace period so a dropped
         host can reclaim them by reconnecting.
   - Backward compatible with v2 clients.
   ============================================================ */

const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const MAX_PLAYERS = 6;
const ROOM_TTL_MS = 1000 * 60 * 30;     // hard cap for very old rooms
const EMPTY_GRACE_MS = 1000 * 45;       // keep an empty room this long (reconnect window)
const SELF_PING_MS = 1000 * 60 * 13;

// code -> { members:[{ws,pid,name,role}], createdAt, emptySince }
const rooms = new Map();

function log(...a){ console.log(new Date().toISOString(), ...a); }

const server = http.createServer((req, res) => {
  if (req.url === '/healthcheck' || req.url === '/') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('ok ' + rooms.size + ' rooms');
    return;
  }
  res.writeHead(404); res.end('not found');
});

const wss = new WebSocketServer({ server });

function send(ws, obj){ if (ws && ws.readyState === ws.OPEN){ try { ws.send(JSON.stringify(obj)); } catch (e) {} } }
function roomOf(ws){ return rooms.get(ws.code); }
function roster(room){ return room.members.map(m => ({ pid:m.pid, name:m.name, role:m.role })); }
function hostOf(room){ return room.members.find(m => m.role === 'host'); }
function broadcastRoster(room){
  const list = roster(room);
  const host = hostOf(room);
  const hostId = host ? host.pid : null;
  for (const m of room.members) send(m.ws, { t:'roster', players:list, hostId });
}
// Remove any existing member that shares this pid (a reconnecting player), closing its old socket.
function removeByPid(room, pid, keepWs){
  const dupes = room.members.filter(m => m.pid === pid && m.ws !== keepWs);
  if (dupes.length){
    room.members = room.members.filter(m => !(m.pid === pid && m.ws !== keepWs));
    for (const d of dupes){ try { d.ws.code = null; d.ws.close(); } catch(e){} }
  }
}
// Ensure the room has exactly one host (promote the oldest member if none).
function ensureHost(room){
  if (!room.members.length) return;
  if (!room.members.some(m => m.role === 'host')){
    room.members[0].role = 'host';
    log('promoted', room.members[0].pid, 'to host');
  }
}
function genCode(){ const AB='ABCDEFGHJKMNPQRSTUVWXYZ23456789';
  let c; do { c = Array.from({length:4}, () => AB[Math.floor(Math.random()*AB.length)]).join(''); } while (rooms.has(c)); return c; }

wss.on('connection', (ws) => {
  ws.isAlive = true; ws.on('pong', () => { ws.isAlive = true; });
  ws.role = null; ws.code = null; ws.pid = null;

  ws.on('message', (raw) => {
    let msg; try { msg = JSON.parse(raw.toString()); } catch (e) { return; }

    // peek: read-only check of whether a room exists and how many people are in it (no join, no side effects)
    if (msg.t === 'peek') {
      const code = (msg.code || '').toUpperCase();
      const room = rooms.get(code);
      send(ws, { t:'peek_result', code, count: room ? room.members.length : 0 });
      return;
    }

    if (msg.t === 'host') {
      let code = (msg.code || '').toUpperCase();
      if (!code || rooms.has(code)) code = genCode();
      ws.role = 'host'; ws.code = code; ws.pid = msg.pid || ('h' + Date.now());
      rooms.set(code, { members: [{ ws, pid: ws.pid, name: msg.name || 'Host', role: 'host' }], createdAt: Date.now(), emptySince: 0 });
      send(ws, { t:'hosted', code, you: ws.pid });
      broadcastRoster(rooms.get(code));
      log('room created', code);
      return;
    }

    if (msg.t === 'join') {
      const code = (msg.code || '').toUpperCase();
      const room = rooms.get(code);
      if (!room) { send(ws, { t:'join_error', reason:'no_room', code }); return; }
      ws.pid = msg.pid || ('g' + Date.now());
      // a reconnecting player with the same pid reclaims their spot rather than duplicating
      removeByPid(room, ws.pid, ws);
      if (room.members.length >= MAX_PLAYERS) { send(ws, { t:'join_error', reason:'full', code }); return; }
      ws.code = code; room.emptySince = 0;
      const wasEmptyOrHostless = !hostOf(room);
      ws.role = wasEmptyOrHostless ? 'host' : 'guest';
      room.members.push({ ws, pid: ws.pid, name: msg.name || 'Player', role: ws.role });
      ensureHost(room);
      // reflect the role the server actually assigned (in case we promoted them)
      const meNow = room.members.find(m => m.ws === ws); if (meNow) ws.role = meNow.role;
      send(ws, { t: ws.role === 'host' ? 'hosted' : 'joined', code, you: ws.pid });
      broadcastRoster(room);
      log('joined', code, '->', room.members.length, 'players', ws.role === 'host' ? '(as host)' : '');
      return;
    }

    if (msg.t === 'relay') {
      const room = roomOf(ws); if (!room) return;
      const me = room.members.find(m => m.ws === ws); if (!me) return;
      if (me.role === 'host') {
        for (const m of room.members) { if (m.ws !== ws) send(m.ws, { t:'relay', fromId: me.pid, payload: msg.payload }); }
      } else {
        const host = hostOf(room);
        if (host) send(host.ws, { t:'relay', fromId: me.pid, payload: msg.payload });
      }
      return;
    }
  });

  ws.on('close', () => {
    const room = roomOf(ws); if (!room) return;
    const me = room.members.find(m => m.ws === ws);
    room.members = room.members.filter(m => m.ws !== ws);
    if (room.members.length === 0){
      // keep the room briefly so a dropped host can reconnect and reclaim it
      room.emptySince = Date.now();
      log('room empty (grace)', ws.code);
      return;
    }
    if (me && me.role === 'host'){
      ensureHost(room);                 // promote the oldest remaining player
      log('host left -> migrated', ws.code);
    }
    broadcastRoster(room);
  });

  ws.on('error', () => {});
});

const heartbeat = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) { try { ws.terminate(); } catch(e){} return; }
    ws.isAlive = false; try { ws.ping(); } catch(e){}
  });
}, 30000);

const sweeper = setInterval(() => {
  const now = Date.now();
  for (const [code, room] of rooms) {
    const open = room.members.filter(m => m.ws.readyState === 1);
    if (open.length === 0){
      const since = room.emptySince || room.createdAt;
      if (now - since > EMPTY_GRACE_MS){ rooms.delete(code); log('room closed (grace expired)', code); }
    } else if (room.createdAt < now - ROOM_TTL_MS){
      rooms.delete(code); log('room expired', code);
    }
  }
}, 1000 * 15);

let selfPingTimer = null;
function startSelfPing(){
  const url = process.env.SELF_URL; if (!url) return;
  selfPingTimer = setInterval(() => {
    const mod = url.startsWith('https') ? require('https') : require('http');
    try { mod.get(url + '/healthcheck', (r) => { r.resume(); }).on('error', () => {}); } catch(e){}
  }, SELF_PING_MS);
}

server.listen(PORT, () => { log('Duel Arcade relay (v3, up to ' + MAX_PLAYERS + ') listening on', PORT); startSelfPing(); });
wss.on('close', () => { clearInterval(heartbeat); clearInterval(sweeper); if (selfPingTimer) clearInterval(selfPingTimer); });
