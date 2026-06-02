/* ============================================================
   Duel Arcade — relay server (v2, multiplayer up to 6)
   - Matchmaking by 4-letter room code.
   - Host is the authority; relays state to all guests.
   - Guests send inputs to the host.
   - Backward compatible with the original 2-player games.
   ============================================================ */

const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const MAX_PLAYERS = 6;
const ROOM_TTL_MS = 1000 * 60 * 30;
const SELF_PING_MS = 1000 * 60 * 13;

// code -> { members:[{ws,pid,name,role}], createdAt }
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
function broadcastRoster(room){
  const list = roster(room);
  const host = room.members.find(m => m.role === 'host');
  const hostId = host ? host.pid : null;
  for (const m of room.members) send(m.ws, { t:'roster', players:list, hostId });
}
function cleanup(code){
  const room = rooms.get(code); if (!room) return;
  room.members = room.members.filter(m => m.ws.readyState === m.ws.OPEN);
  if (room.members.length === 0){ rooms.delete(code); log('room closed', code); }
}
function genCode(){ const AB='ABCDEFGHJKMNPQRSTUVWXYZ23456789';
  let c; do { c = Array.from({length:4}, () => AB[Math.floor(Math.random()*AB.length)]).join(''); } while (rooms.has(c)); return c; }

wss.on('connection', (ws) => {
  ws.isAlive = true; ws.on('pong', () => { ws.isAlive = true; });
  ws.role = null; ws.code = null; ws.pid = null;

  ws.on('message', (raw) => {
    let msg; try { msg = JSON.parse(raw.toString()); } catch (e) { return; }

    if (msg.t === 'host') {
      let code = (msg.code || '').toUpperCase();
      if (!code || rooms.has(code)) code = genCode();
      ws.role = 'host'; ws.code = code; ws.pid = msg.pid || ('h' + Date.now());
      rooms.set(code, { members: [{ ws, pid: ws.pid, name: msg.name || 'Host', role: 'host' }], createdAt: Date.now() });
      send(ws, { t:'hosted', code, you: ws.pid });
      broadcastRoster(rooms.get(code));
      log('room created', code);
      return;
    }

    if (msg.t === 'join') {
      const code = (msg.code || '').toUpperCase();
      const room = rooms.get(code);
      if (!room) { send(ws, { t:'join_error', reason:'no_room', code }); return; }
      if (room.members.length >= MAX_PLAYERS) { send(ws, { t:'join_error', reason:'full', code }); return; }
      ws.role = 'guest'; ws.code = code; ws.pid = msg.pid || ('g' + Date.now());
      room.members.push({ ws, pid: ws.pid, name: msg.name || 'Player', role: 'guest' });
      send(ws, { t:'joined', code, you: ws.pid });
      broadcastRoster(room);
      log('joined', code, '->', room.members.length, 'players');
      return;
    }

    if (msg.t === 'relay') {
      const room = roomOf(ws); if (!room) return;
      const me = room.members.find(m => m.ws === ws); if (!me) return;
      if (me.role === 'host') {
        for (const m of room.members) { if (m.ws !== ws) send(m.ws, { t:'relay', fromId: me.pid, payload: msg.payload }); }
      } else {
        const host = room.members.find(m => m.role === 'host');
        if (host) send(host.ws, { t:'relay', fromId: me.pid, payload: msg.payload });
      }
      return;
    }
  });

  ws.on('close', () => {
    const room = roomOf(ws); if (!room) return;
    const me = room.members.find(m => m.ws === ws);
    room.members = room.members.filter(m => m.ws !== ws);
    if (me && me.role === 'host') {
      for (const m of room.members) send(m.ws, { t:'host_left' });
      rooms.delete(ws.code);
      log('host left, room closed', ws.code);
    } else {
      if (room.members.length > 0) broadcastRoster(room);
      cleanup(ws.code);
    }
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
  const cutoff = Date.now() - ROOM_TTL_MS;
  for (const [code, room] of rooms) {
    const anyOpen = room.members.some(m => m.ws.readyState === 1);
    if (!anyOpen && room.createdAt < cutoff) { rooms.delete(code); log('room expired', code); }
  }
}, 1000 * 60 * 5);

let selfPingTimer = null;
function startSelfPing(){
  const url = process.env.SELF_URL; if (!url) return;
  selfPingTimer = setInterval(() => {
    const mod = url.startsWith('https') ? require('https') : require('http');
    try { mod.get(url + '/healthcheck', (r) => { r.resume(); }).on('error', () => {}); } catch(e){}
  }, SELF_PING_MS);
}

server.listen(PORT, () => { log('Duel Arcade relay (v2, up to ' + MAX_PLAYERS + ') listening on', PORT); startSelfPing(); });
wss.on('close', () => { clearInterval(heartbeat); clearInterval(sweeper); if (selfPingTimer) clearInterval(selfPingTimer); });
