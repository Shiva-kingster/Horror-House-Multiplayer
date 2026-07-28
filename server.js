/* ============================================================================================
   HORROR HOUSE — Multiplayer Server (Node.js + Socket.IO)
   ============================================================================================
   Pure networking relay/room-host, exactly mirroring the shape of the game's previous
   Firebase Firestore layer (room doc + players sub-collection) so the client's game logic,
   rendering, Ghost AI, inventory, etc. never had to change — only the transport did.

   Authority model (unchanged from before): the HOST client still simulates the Ghost AI and
   owns room-level state transitions; this server does not run any game logic itself. It is a
   room manager + real-time relay: it stores each room's last-known state (so late joiners /
   reconnecting players get a snapshot) and broadcasts updates to everyone in the room,
   including the sender (matching Firestore's onSnapshot behavior, which the client relies on
   to detect its own writes taking effect, e.g. host starting the game).

   Sections:
     1. Room store (in-memory)
     2. Helpers
     3. Socket.IO event handlers
     4. Disconnect / reconnect grace period
     5. HTTP server bootstrap
   ============================================================================================ */
const http = require('http');
const express = require('express');
const { Server } = require('socket.io');

const PORT = process.env.PORT || 3001;
const MAX_PLAYERS = 4;
const RECONNECT_GRACE_MS = 30000; // players who disconnect get 30s to reconnect before their slot is freed
const ROOM_TTL_MS = 1000 * 60 * 60 * 6; // rooms with no activity for 6h are garbage-collected

const app = express();
app.get('/', (req, res) => res.send('Horror House multiplayer server is running.'));
app.get('/health', (req, res) => res.json({ ok: true, rooms: Object.keys(rooms).length }));

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  pingInterval: 10000,
  pingTimeout: 8000
});

/* ---------------------------------- 1. ROOM STORE (in-memory) ---------------------------------- */
// rooms[code] = {
//   hostId, status, seed, level, maxPlayers, keysCollected, collectedItemIds, doorUnlocked,
//   gameStartTime, gameDuration, ghost, lastEvent, lightsOff, winner, createdAt, lastActivity,
//   players: { [playerId]: {...playerFields, ready, connected, socketId} },
//   disconnectTimers: { [playerId]: NodeJS.Timeout }
// }
const rooms = {};
// quick lookup: socket.id -> {code, playerId}
const socketIndex = {};

/* ---------------------------------- 2. HELPERS ---------------------------------- */
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
function randRoomCode() {
  let out = '';
  for (let i = 0; i < 6; i++) out += CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)];
  return out;
}
function uniqueRoomCode() {
  let code;
  do { code = randRoomCode(); } while (rooms[code]);
  return code;
}
function hashStringToSeed(str) {
  let h = 1779033703 ^ str.length;
  for (let i = 0; i < str.length; i++) {
    h = Math.imul(h ^ str.charCodeAt(i), 3432918353);
    h = (h << 13) | (h >>> 19);
  }
  return (h ^ (h >>> 16)) >>> 0;
}
function newPlayer(name, slotIndex) {
  const colors = ['#f0b545', '#e23b3b', '#5fb3d9', '#7fd97f'];
  return {
    name: String(name || 'Survivor').slice(0, 14), color: colors[slotIndex % 4], slotIndex,
    x: 0, y: 0, angle: 0, health: 100, battery: 100, stamina: 100,
    inventory: { battery: 0, firstaid: 0, flashbomb: 0, noisemaker: 0 }, keys: 0,
    status: 'alive', talking: false, ready: false, connected: true, lastUpdate: Date.now()
  };
}
function roomSnapshot(room) {
  // the "room document" shape the client expects (players sent separately)
  const { players, disconnectTimers, ...doc } = room;
  return doc;
}
function playersSnapshot(room) {
  const out = {};
  for (const pid in room.players) {
    const { socketId, ...pub } = room.players[pid]; // don't leak internal socket ids to clients
    out[pid] = pub;
  }
  return out;
}
function broadcastRoom(code) {
  const room = rooms[code];
  if (!room) return;
  io.to(code).emit('roomUpdate', roomSnapshot(room));
}
function broadcastPlayers(code) {
  const room = rooms[code];
  if (!room) return;
  io.to(code).emit('playersUpdate', playersSnapshot(room));
}
function touch(room) { room.lastActivity = Date.now(); }

/* ---------------------------------- 3. SOCKET.IO EVENT HANDLERS ---------------------------------- */
io.on('connection', (socket) => {

  socket.on('createRoom', ({ name } = {}, ack) => {
    const code = uniqueRoomCode();
    const playerId = socket.id + ':' + Date.now().toString(36);
    const seed = hashStringToSeed(code + Date.now());
    const room = {
      hostId: playerId, status: 'lobby', seed, level: 1, maxPlayers: MAX_PLAYERS,
      keysCollected: 0, collectedItemIds: [], doorUnlocked: false,
      gameStartTime: null, gameDuration: 600,
      ghost: { x: 0, y: 0, mode: 'patrol', visible: true, ts: Date.now() },
      lastEvent: { type: 'none', ts: 0, x: 0, y: 0 },
      lightsOff: false, winner: null, createdAt: Date.now(), lastActivity: Date.now(),
      players: {}, disconnectTimers: {}
    };
    room.players[playerId] = newPlayer(name, 0);
    rooms[code] = room;

    socket.join(code);
    socketIndex[socket.id] = { code, playerId };

    if (typeof ack === 'function') {
      ack({ ok: true, code, playerId, seed, roomData: roomSnapshot(room) });
    }
  });

  socket.on('joinRoom', ({ code, name } = {}, ack) => {
    code = String(code || '').toUpperCase().trim();
    const room = rooms[code];
    const fail = (error) => { if (typeof ack === 'function') ack({ ok: false, error }); };
    if (!room) return fail('Room not found.');
    if (room.status !== 'lobby') return fail('That game has already started.');
    const activeCount = Object.values(room.players).filter(p => p.connected !== false).length;
    if (activeCount >= room.maxPlayers) return fail('Room is full (4/4).');

    const playerId = socket.id + ':' + Date.now().toString(36);
    const slotIndex = Object.keys(room.players).length;
    room.players[playerId] = newPlayer(name, slotIndex);
    touch(room);

    socket.join(code);
    socketIndex[socket.id] = { code, playerId };

    if (typeof ack === 'function') {
      ack({ ok: true, code, playerId, seed: room.seed, roomData: roomSnapshot(room) });
    }
    broadcastPlayers(code);
  });

  // Reconnect: client stores {code, playerId} locally and tries to resume the same slot
  // after a dropped connection instead of being treated as a brand new player.
  socket.on('reconnectPlayer', ({ code, playerId, name } = {}, ack) => {
    code = String(code || '').toUpperCase().trim();
    const room = rooms[code];
    const fail = (error) => { if (typeof ack === 'function') ack({ ok: false, error }); };
    if (!room || !room.players[playerId]) return fail('Room no longer exists.');

    if (room.disconnectTimers[playerId]) {
      clearTimeout(room.disconnectTimers[playerId]);
      delete room.disconnectTimers[playerId];
    }
    room.players[playerId].connected = true;
    if (name) room.players[playerId].name = String(name).slice(0, 14);
    touch(room);

    socket.join(code);
    socketIndex[socket.id] = { code, playerId };

    if (typeof ack === 'function') {
      ack({ ok: true, code, playerId, seed: room.seed, roomData: roomSnapshot(room), isHost: room.hostId === playerId });
    }
    broadcastPlayers(code);
    io.to(code).emit('toast', { text: `${room.players[playerId].name} reconnected.` });
  });

  socket.on('setReady', ({ code, playerId, ready } = {}) => {
    const room = rooms[code];
    if (!room || !room.players[playerId]) return;
    room.players[playerId].ready = !!ready;
    touch(room);
    broadcastPlayers(code);
  });

  // High-frequency movement + flashlight-direction channel — deliberately NOT echoed back
  // to the sender (they already have their own authoritative position) and kept to a tiny
  // payload for bandwidth: just position + facing angle, nothing else.
  socket.on('playerMove', ({ code, playerId, x, y, angle } = {}) => {
    const room = rooms[code];
    if (!room || !room.players[playerId]) return;
    const p = room.players[playerId];
    p.x = x; p.y = y; p.angle = angle;
    socket.to(code).emit('playerMoved', { playerId, x, y, angle });
  });

  // Occasional full-field player updates (health/battery/stamina/inventory/keys/status/talking)
  socket.on('updatePlayer', ({ code, playerId, partial } = {}) => {
    const room = rooms[code];
    if (!room || !room.players[playerId] || !partial) return;
    Object.assign(room.players[playerId], partial, { lastUpdate: Date.now() });
    touch(room);
    broadcastPlayers(code);
  });

  socket.on('setPlayer', ({ code, playerId, data } = {}) => {
    const room = rooms[code];
    if (!room || !data) return;
    room.players[playerId] = Object.assign(room.players[playerId] || newPlayer(data.name, 0), data);
    touch(room);
    broadcastPlayers(code);
  });

  // Room-level state: game start, keys, door, ghost AI state, timer, events, etc.
  socket.on('updateRoom', ({ code, partial } = {}) => {
    const room = rooms[code];
    if (!room || !partial) return;
    Object.assign(room, partial);
    touch(room);
    broadcastRoom(code);
  });

  socket.on('leaveRoom', ({ code, playerId } = {}) => {
    removePlayerFromRoom(code, playerId);
    socket.leave(code);
    delete socketIndex[socket.id];
  });

  socket.on('disconnect', () => {
    const entry = socketIndex[socket.id];
    if (!entry) return;
    const { code, playerId } = entry;
    const room = rooms[code];
    delete socketIndex[socket.id];
    if (!room || !room.players[playerId]) return;

    // grace period instead of an instant removal, so a flaky connection / tab refresh
    // doesn't immediately boot the player out of an in-progress game
    room.players[playerId].connected = false;
    broadcastPlayers(code);
    io.to(code).emit('toast', { text: `${room.players[playerId].name} disconnected — waiting to reconnect...` });

    room.disconnectTimers[playerId] = setTimeout(() => {
      removePlayerFromRoom(code, playerId, true);
    }, RECONNECT_GRACE_MS);
  });

  function removePlayerFromRoom(code, playerId, timedOut) {
    const room = rooms[code];
    if (!room || !room.players[playerId]) return;
    const wasHost = room.hostId === playerId;
    const name = room.players[playerId].name;
    delete room.players[playerId];
    if (room.disconnectTimers[playerId]) { clearTimeout(room.disconnectTimers[playerId]); delete room.disconnectTimers[playerId]; }

    const remaining = Object.keys(room.players);
    if (remaining.length === 0) {
      delete rooms[code]; // empty room — clean up
      return;
    }
    if (wasHost) {
      // host migration: promote the next player in slot order so the room can keep going
      remaining.sort((a, b) => (room.players[a].slotIndex||0) - (room.players[b].slotIndex||0));
      room.hostId = remaining[0];
    }
    touch(room);
    broadcastPlayers(code);
    broadcastRoom(code);
    io.to(code).emit('toast', { text: `${name} ${timedOut ? 'left' : 'disconnected'}.` });
  }
});

/* ---------------------------------- 4. Idle room garbage collection ---------------------------------- */
setInterval(() => {
  const now = Date.now();
  for (const code in rooms) {
    if (now - rooms[code].lastActivity > ROOM_TTL_MS) delete rooms[code];
  }
}, 60 * 60 * 1000);

/* ---------------------------------- 5. HTTP SERVER BOOTSTRAP ---------------------------------- */
server.listen(PORT, () => {
  console.log(`Horror House multiplayer server listening on port ${PORT}`);
});
