/**
 * LizardCraft multiplayer server
 * - Daily world map (UTC day) — builds reset each day
 * - Accounts: nick + password, inventory persists across days
 * - One session per nick (no second tab/device while online)
 */
const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const WORLD_DIR = process.env.WORLD_DIR || path.join(__dirname, "worlds");
const ACCOUNTS_FILE = path.join(WORLD_DIR, "_accounts.json");
const SAVE_EVERY_MS = 8000;

try { fs.mkdirSync(WORLD_DIR, { recursive: true }); } catch (e) {}

function utcDay() {
  return new Date().toISOString().slice(0, 10); // YYYY-MM-DD
}
function daySeed(day) {
  let h = 2166136261;
  for (let i = 0; i < day.length; i++) h = Math.imul(h ^ day.charCodeAt(i), 16777619);
  return (h >>> 0) || 1;
}
function safeNick(n) {
  return String(n || "").replace(/[^\w\u0400-\u04FF\-]/g, "").slice(0, 16);
}
function hashPass(pass, salt) {
  return crypto.createHash("sha256").update(String(pass) + ":" + salt + ":lc").digest("hex");
}

/* ---------- accounts ---------- */
let accounts = {}; // nick -> { salt, hash, inv, armor, sel }
function loadAccounts() {
  try {
    accounts = JSON.parse(fs.readFileSync(ACCOUNTS_FILE, "utf8")) || {};
  } catch (e) {
    accounts = {};
  }
}
function saveAccounts() {
  try {
    fs.writeFileSync(ACCOUNTS_FILE, JSON.stringify(accounts));
  } catch (e) {
    console.warn("accounts save failed", e.message);
  }
}
loadAccounts();

/* ---------- daily worlds ---------- */
function worldPath(code, day) {
  const safe = String(code || "GLOBAL").replace(/[^a-zA-Z0-9_-]/g, "_").slice(0, 64);
  return path.join(WORLD_DIR, safe + "_" + day + ".json");
}
function loadWorld(code, day) {
  try {
    const raw = fs.readFileSync(worldPath(code, day), "utf8");
    const data = JSON.parse(raw);
    return {
      edits: data.edits || {},
      beds: data.beds || [],
      torchFaces: data.torchFaces || [],
      ladderFaces: data.ladderFaces || [],
      chests: data.chests || [],
      claims: data.claims || [],
    };
  } catch (e) {
    return { edits: {}, beds: [], torchFaces: [], ladderFaces: [], chests: [], claims: [] };
  }
}
function saveWorld(code, room) {
  try {
    const data = {
      v: 2,
      code,
      day: room.day,
      seed: room.seed,
      savedAt: Date.now(),
      edits: room.edits || {},
      beds: room.beds || [],
      torchFaces: room.torchFaces || [],
      ladderFaces: room.ladderFaces || [],
      chests: room.chests || [],
      claims: room.claims || [],
    };
    fs.writeFileSync(worldPath(code, room.day), JSON.stringify(data));
  } catch (e) {
    console.warn("save failed", code, e.message);
  }
}

function applyBlk(edits, x, y, z, t) {
  const CS = 16, H = 128;
  if (y < 0 || y >= H) return;
  const cx = x >> 4, cz = z >> 4;
  const lx = x & 15, lz = z & 15;
  const i = (y * CS + lz) * CS + lx;
  const key = cx + "," + cz;
  let arr = edits[key];
  if (!arr) arr = edits[key] = [];
  let found = false;
  for (let j = 0; j < arr.length; j += 2) {
    if (arr[j] === i) { arr[j + 1] = t; found = true; break; }
  }
  if (!found) arr.push(i, t);
}

const rooms = new Map(); // code -> room
const onlineNicks = new Map(); // nickLower -> ws  (global one-session lock)

function getRoom(code) {
  code = String(code || "GLOBAL");
  const day = utcDay();
  let r = rooms.get(code);
  /* rotate room if day changed */
  if (r && r.day !== day) {
    saveWorld(code, r);
    for (const c of r.clients) {
      try { c.send(JSON.stringify({ type: "err", code: "new_day", msg: "New day — reconnect for a fresh map" })); } catch (e) {}
      try { c.close(); } catch (e) {}
    }
    rooms.delete(code);
    r = null;
  }
  if (!r) {
    const data = loadWorld(code, day);
    r = {
      code,
      day,
      seed: daySeed(day),
      clients: new Set(),
      edits: data.edits,
      beds: data.beds,
      torchFaces: data.torchFaces,
      ladderFaces: data.ladderFaces,
      chests: data.chests,
      claims: data.claims,
      dirty: false,
      hostId: null,
    };
    rooms.set(code, r);
    console.log("room open", code, "day", day, "seed", r.seed);
  }
  return r;
}

function broadcast(room, msg, except) {
  const raw = JSON.stringify(msg);
  for (const c of room.clients) {
    if (c !== except && c.readyState === 1) {
      try { c.send(raw); } catch (e) {}
    }
  }
}

const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("LizardCraft MP OK\n");
});
const wss = new WebSocketServer({ server });

let nextId = 1;

wss.on("connection", (ws) => {
  ws.id = "p" + (nextId++);
  ws.nick = null;
  ws.authed = false;
  ws.roomCode = null;

  ws.on("message", (buf) => {
    let msg;
    try { msg = JSON.parse(buf.toString()); } catch (e) { return; }
    if (!msg || typeof msg !== "object") return;

    /* ---- register ---- */
    if (msg.type === "register") {
      const nick = safeNick(msg.nick);
      const pass = String(msg.pass || "");
      if (!nick || nick.length < 2) {
        ws.send(JSON.stringify({ type: "auth", ok: false, code: "bad_nick", msg: "Nick must be 2–16 characters" }));
        return;
      }
      if (pass.length < 3) {
        ws.send(JSON.stringify({ type: "auth", ok: false, code: "bad_pass", msg: "Password min 3 characters" }));
        return;
      }
      const key = nick.toLowerCase();
      if (accounts[key]) {
        ws.send(JSON.stringify({ type: "auth", ok: false, code: "taken", msg: "This nickname is already registered" }));
        return;
      }
      const salt = crypto.randomBytes(8).toString("hex");
      accounts[key] = { nick, salt, hash: hashPass(pass, salt), inv: null, armor: null, sel: 0, created: Date.now() };
      saveAccounts();
      ws.send(JSON.stringify({ type: "auth", ok: true, mode: "register", msg: "Registered! You can log in now." }));
      return;
    }

    /* ---- login (before join) ---- */
    if (msg.type === "login") {
      const nick = safeNick(msg.nick);
      const pass = String(msg.pass || "");
      const key = nick.toLowerCase();
      const acc = accounts[key];
      if (!acc) {
        ws.send(JSON.stringify({ type: "auth", ok: false, code: "no_user", msg: "Account not found — register first" }));
        return;
      }
      if (hashPass(pass, acc.salt) !== acc.hash) {
        ws.send(JSON.stringify({ type: "auth", ok: false, code: "bad_pass", msg: "Wrong password" }));
        return;
      }
      /* already online? */
      const existing = onlineNicks.get(key);
      if (existing && existing !== ws && existing.readyState === 1) {
        ws.send(JSON.stringify({ type: "auth", ok: false, code: "online", msg: "This account is already online (another tab/device)" }));
        return;
      }
      ws.nick = acc.nick || nick;
      ws.authed = true;
      onlineNicks.set(key, ws);
      ws.send(JSON.stringify({
        type: "auth",
        ok: true,
        mode: "login",
        nick: ws.nick,
        inv: acc.inv || null,
        armor: acc.armor || null,
        sel: acc.sel || 0,
        msg: "Logged in",
      }));
      return;
    }

    /* ---- join room (must be authed) ---- */
    if (msg.type === "join") {
      if (!ws.authed || !ws.nick) {
        ws.send(JSON.stringify({ type: "err", code: "auth", msg: "Login required" }));
        return;
      }
      const key = ws.nick.toLowerCase();
      const existing = onlineNicks.get(key);
      if (existing && existing !== ws && existing.readyState === 1) {
        ws.send(JSON.stringify({ type: "err", code: "online", msg: "Already online in another tab" }));
        try { ws.close(); } catch (e) {}
        return;
      }
      onlineNicks.set(key, ws);

      const code = String(msg.code || "GLOBAL");
      const room = getRoom(code);
      ws.roomCode = code;
      room.clients.add(ws);
      if (!room.hostId) room.hostId = ws.id;

      try {
        ws.send(JSON.stringify({
          type: "welcome",
          id: ws.id,
          host: room.hostId === ws.id,
          day: room.day,
          seed: room.seed,
          edits: room.edits,
          beds: room.beds,
          torchFaces: room.torchFaces,
          ladderFaces: room.ladderFaces,
          chests: room.chests,
          claims: room.claims,
        }));
      } catch (e) {}
      broadcast(room, { type: "peer", id: ws.id, nick: ws.nick }, ws);
      return;
    }

    const room = ws.roomCode ? rooms.get(ws.roomCode) : null;
    if (!room || !ws.authed) return;

    /* persist player inventory */
    if (msg.type === "player_save") {
      const key = ws.nick.toLowerCase();
      const acc = accounts[key];
      if (acc) {
        if (Array.isArray(msg.inv)) acc.inv = msg.inv;
        if (Array.isArray(msg.armor)) acc.armor = msg.armor;
        if (msg.sel != null) acc.sel = msg.sel | 0;
        saveAccounts();
      }
      return;
    }

    if (msg.type === "blk") {
      const x = msg.x | 0, y = msg.y | 0, z = msg.z | 0, t = msg.t | 0;
      applyBlk(room.edits, x, y, z, t);
      room.dirty = true;
      broadcast(room, { type: "blk", x, y, z, t, id: ws.id }, ws);
      return;
    }

    if (msg.type === "world_save") {
      if (msg.edits && typeof msg.edits === "object") room.edits = msg.edits;
      if (Array.isArray(msg.beds)) room.beds = msg.beds;
      if (Array.isArray(msg.torchFaces)) room.torchFaces = msg.torchFaces;
      if (Array.isArray(msg.ladderFaces)) room.ladderFaces = msg.ladderFaces;
      if (Array.isArray(msg.chests)) room.chests = msg.chests;
      if (Array.isArray(msg.claims)) {
        room.claims = msg.claims;
        broadcast(room, { type: "claims", claims: room.claims }, ws);
      }
      room.dirty = true;
      saveWorld(room.code, room);
      return;
    }

    if (msg.type === "pres") {
      msg.id = ws.id;
      if (ws.nick) msg.nick = ws.nick;
      broadcast(room, msg, ws);
      return;
    }

    if (msg.type === "chat") {
      broadcast(room, {
        type: "chat",
        text: String(msg.text || "").slice(0, 100),
        nick: ws.nick || "?",
        id: ws.id,
      });
      return;
    }

    if (msg.type === "drop" || msg.type === "pickup" || msg.type === "hit" || msg.type === "door") {
      msg.id = msg.id || ws.id;
      broadcast(room, msg, ws);
      return;
    }
  });

  ws.on("close", () => {
    if (ws.nick) {
      const key = ws.nick.toLowerCase();
      if (onlineNicks.get(key) === ws) onlineNicks.delete(key);
    }
    const code = ws.roomCode;
    if (!code) return;
    const room = rooms.get(code);
    if (!room) return;
    room.clients.delete(ws);
    broadcast(room, { type: "bye", id: ws.id });
    if (room.hostId === ws.id) room.hostId = [...room.clients][0]?.id || null;
    if (room.clients.size === 0) {
      saveWorld(room.code, room);
      rooms.delete(room.code);
      console.log("room saved & closed", code, room.day);
    }
  });
});

setInterval(() => {
  for (const room of rooms.values()) {
    if (room.dirty) {
      saveWorld(room.code, room);
      room.dirty = false;
    }
  }
  /* day rollover check */
  const day = utcDay();
  for (const [code, room] of [...rooms.entries()]) {
    if (room.day !== day) {
      saveWorld(code, room);
      for (const c of room.clients) {
        try { c.send(JSON.stringify({ type: "err", code: "new_day", msg: "New day — map reset. Reconnect!" })); } catch (e) {}
        try { c.close(); } catch (e) {}
      }
      rooms.delete(code);
    }
  }
}, SAVE_EVERY_MS);

server.listen(PORT, () => console.log("LizardCraft MP on :" + PORT, "worlds →", WORLD_DIR));
