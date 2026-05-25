const https = require('https');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const ROOT = __dirname;

const mime = {
  '.html': 'text/html', '.js': 'application/javascript', '.css': 'text/css',
  '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.png': 'image/png',
  '.gif': 'image/gif', '.svg': 'image/svg+xml', '.ico': 'image/x-icon',
};

const server = https.createServer({
  key: fs.readFileSync(path.join(ROOT, 'key.pem')),
  cert: fs.readFileSync(path.join(ROOT, 'cert.pem')),
}, (req, res) => {
  let file = req.url === '/' ? '/index.html' : req.url.split('?')[0];
  let filePath = path.join(ROOT, file);

  if (!filePath.startsWith(ROOT + path.sep) && filePath !== path.join(ROOT, 'index.html')) {
    res.writeHead(403); res.end('Forbidden'); return;
  }

  if (!fs.existsSync(filePath) || fs.statSync(filePath).isDirectory()) {
    res.writeHead(404); res.end('Not found'); return;
  }

  const ext = path.extname(filePath).toLowerCase();
  res.writeHead(200, {
    'Content-Type': mime[ext] || 'application/octet-stream',
    'Cache-Control': 'no-cache, no-store, must-revalidate',
    'Pragma': 'no-cache',
    'Expires': '0',
  });
  fs.createReadStream(filePath).pipe(res);
});

// ─── WebSocket 信令中继 ──────────────────────────

const wss = new WebSocketServer({ noServer: true });
const rooms = new Map(); // roomCode -> { host, guest, roomCode }

function findRoom(ws) {
  for (const [code, room] of rooms) {
    if (room.host === ws || room.guest === ws) return room;
  }
  return null;
}

wss.on('connection', (ws) => {
  ws.on('message', (raw, isBinary) => {
    // 二进制帧或非 JSON 数据直接转发
    if (isBinary || !(raw.toString().startsWith('{'))) {
      const room = findRoom(ws);
      if (room) {
        const other = room.host === ws ? room.guest : room.host;
        if (other) other.send(raw);
      }
      if (!isBinary) console.log('[信令] 非 JSON 文本帧，已转发');
      return;
    }

    let msg;
    try { msg = JSON.parse(raw.toString()); } catch (e) { return; }

    switch (msg.type) {

      case 'create': {
        if (rooms.has(msg.room)) {
          ws.send(JSON.stringify({ type: 'error', message: '房间号已被占用' }));
          return;
        }
        rooms.set(msg.room, { host: ws, guest: null, roomCode: msg.room });
        ws.send(JSON.stringify({ type: 'created' }));
        break;
      }

      case 'join': {
        const room = rooms.get(msg.room);
        if (!room || room.guest) {
          ws.send(JSON.stringify({ type: 'error', message: '房间不存在或已满员' }));
          return;
        }
        room.guest = ws;
        ws.send(JSON.stringify({ type: 'joined' }));
        room.host.send(JSON.stringify({ type: 'peer-joined' }));
        break;
      }

      case 'offer':
      case 'answer':
      case 'ice-candidate':
      case 'hangup': {
        console.log('[信令] 收到 ' + msg.type + ' from ' + (findRoom(ws) ? (findRoom(ws).host === ws ? 'host' : 'guest') : '?'));
        const room = findRoom(ws);
        if (!room) { console.log('[信令] 房间不存在，丢弃'); return; }
        const other = room.host === ws ? room.guest : room.host;
        if (other) {
          other.send(raw.toString());
          console.log('[信令] ' + msg.type + ' 已转发');
        } else {
          console.log('[信令] 对方未连接，丢弃');
        }
        break;
      }
    }
  });

  ws.on('close', () => {
    const room = findRoom(ws);
    if (!room) return;
    const other = room.host === ws ? room.guest : room.host;
    if (other) {
      other.send(JSON.stringify({ type: 'hangup' }));
    }
    rooms.delete(room.roomCode);
  });

  ws.on('error', () => {});
});

server.on('upgrade', (req, socket, head) => {
  wss.handleUpgrade(req, socket, head, (ws) => {
    wss.emit('connection', ws, req);
  });
});

// ──────────────────────────────────────────────────

server.listen(3456, '0.0.0.0', () => {
  console.log('HTTPS server: https://localhost:3456');
  console.log('WebSocket signaling ready');
});
