'use strict';

/*
 * Startup Company Multiplayer 16 - merkezi oda sunucusu
 *
 * Tüm odalar burada tutulur; oyuncular IP bilmeden oda listesinden katılır.
 * Taşıma: WebSocket, her mesaj bir JSON nesnesi.
 *
 *   npm install && npm start            (PORT ortam değişkeni, varsayılan 8080)
 *   GET /health                          -> { ok, rooms, players }
 */

const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PROTOCOL_VERSION = 4;
const HARD_MAX_PLAYERS = 16;
const MAX_ROOMS = 300;
const MAX_CONNECTIONS_PER_IP = 24;
const MAX_MESSAGE_BYTES = 64 * 1024;
const CHAT_HISTORY = 60;
const EMPTY_GAME_ROOM_TTL_MS = 30 * 60 * 1000;
const RATE_WINDOW_MS = 10000;
const RATE_SOFT_LIMIT = 120;
const RATE_HARD_LIMIT = 600;

const DEFAULT_SETTINGS = {
    startingMoney: 15000,
    daysPerYear: 50,
    disableInvestor: false,
    disableWorkstationLimit: false,
    syncClock: true,
    clientsCanPause: true,
    waitForSlowPlayers: true,
    allowLateJoin: true,
    moneyTransfers: true
};
const IN_GAME_SETTINGS = ['syncClock', 'clientsCanPause', 'waitForSlowPlayers', 'allowLateJoin', 'moneyTransfers'];

function randomId(bytes) {
    return crypto.randomBytes(bytes || 6).toString('hex');
}

function clean(value, maxLength) {
    return String(value === undefined || value === null ? '' : value)
        .replace(/[\u0000-\u001F\u007F]/g, '')
        .trim()
        .slice(0, maxLength);
}

function clampInt(value, min, max, fallback) {
    const number = Math.round(Number(value));
    if (!isFinite(number)) return fallback;
    return Math.max(min, Math.min(max, number));
}

function finiteOrNull(value) {
    if (value === null || value === undefined || value === '' || typeof value === 'boolean') return null;
    const number = Number(value);
    return isFinite(number) ? number : null;
}

function sanitizeProfile(profile) {
    const p = profile || {};
    return {
        name: clean(p.name, 24) || 'Oyuncu',
        companyName: clean(p.companyName, 40) || 'Yeni Şirket',
        ceoName: clean(p.ceoName, 40) || 'CEO',
        websiteName: clean(p.websiteName, 40) || 'MyStartup',
        productTypeName: clean(p.productTypeName, 40),
        logoNumber: clampInt(p.logoNumber, 1, 99, 1),
        avatar: /^(male|female)[1-3]$/.test(String(p.avatar)) ? String(p.avatar) : 'male1',
        backstory: ['Designer', 'Developer', 'Manager'].indexOf(p.backstory) >= 0 ? p.backstory : 'Developer'
    };
}

function sanitizeSettings(input, current) {
    const s = Object.assign({}, current || DEFAULT_SETTINGS);
    const i = input || {};
    if (i.startingMoney !== undefined) s.startingMoney = clampInt(i.startingMoney, 0, 5000000000, s.startingMoney);
    if (i.daysPerYear !== undefined) s.daysPerYear = clampInt(i.daysPerYear, 10, 365, s.daysPerYear);
    ['disableInvestor', 'disableWorkstationLimit'].concat(IN_GAME_SETTINGS).forEach(function (key) {
        if (i[key] !== undefined) s[key] = Boolean(i[key]);
    });
    return s;
}

function sanitizeStats(input) {
    const i = input || {};
    return {
        balance: finiteOrNull(i.balance),
        valuation: finiteOrNull(i.valuation),
        users: finiteOrNull(i.users),
        employees: finiteOrNull(i.employees),
        products: clampInt(i.products, 0, 99, 0),
        productName: clean(i.productName, 40),
        dateMs: finiteOrNull(i.dateMs),
        day: finiteOrNull(i.day),
        paused: Boolean(i.paused),
        speed: clampInt(i.speed, 1, 3, 1),
        inMenu: Boolean(i.inMenu)
    };
}

function createLobbyServer(options) {
    const opts = options || {};
    const log = typeof opts.log === 'function' ? opts.log : function () {};
    const rooms = new Map();          // roomId -> room
    const connections = new Set();
    const ipCounts = new Map();
    let listTimer = null;

    // ------------------------------------------------------------ yardımcılar

    function send(conn, message) {
        if (!conn || !conn.ws || conn.ws.readyState !== 1) return;
        try {
            conn.ws.send(JSON.stringify(message));
        } catch (error) {
            log('gönderme hatası: ' + error.message);
        }
    }

    function players(room) {
        return Array.from(room.players.values()).sort(function (a, b) { return a.joinedAt - b.joinedAt; });
    }

    function onlinePlayers(room) {
        return players(room).filter(function (p) { return p.conn; });
    }

    function playerById(room, id) {
        return players(room).find(function (p) { return p.id === id; }) || null;
    }

    function publicPlayer(room, p) {
        return {
            id: p.id,
            name: p.profile.name,
            companyName: p.profile.companyName,
            ceoName: p.profile.ceoName,
            websiteName: p.profile.websiteName,
            productTypeName: p.profile.productTypeName,
            logoNumber: p.profile.logoNumber,
            avatar: p.profile.avatar,
            backstory: p.profile.backstory,
            ready: p.ready,
            online: Boolean(p.conn),
            isHost: p.id === room.hostId,
            loaded: p.loaded,
            ping: p.ping,
            stats: p.stats
        };
    }

    function publicRoom(room) {
        return {
            id: room.id,
            name: room.name,
            hasPassword: Boolean(room.password),
            maxPlayers: room.maxPlayers,
            hostId: room.hostId,
            phase: room.phase,
            settings: room.settings,
            session: room.session,
            players: players(room).map(function (p) { return publicPlayer(room, p); })
        };
    }

    function roomSummary(room) {
        const host = playerById(room, room.hostId);
        return {
            id: room.id,
            name: room.name,
            hostName: host ? host.profile.name : '',
            hasPassword: Boolean(room.password),
            online: onlinePlayers(room).length,
            slots: room.players.size,
            maxPlayers: room.maxPlayers,
            phase: room.phase,
            allowLateJoin: room.settings.allowLateJoin,
            sessionId: room.session ? room.session.id : null,
            createdAt: room.createdAt
        };
    }

    function roomList() {
        return Array.from(rooms.values())
            .filter(function (room) { return onlinePlayers(room).length > 0; })
            .sort(function (a, b) { return b.createdAt - a.createdAt; })
            .map(roomSummary);
    }

    function sendList(conn) {
        send(conn, { type: 'rooms', rooms: roomList() });
    }

    // Oda listesindeki değişiklikleri odaya girmemiş herkese (en fazla saniyede bir) gönder.
    function broadcastListSoon() {
        if (listTimer) return;
        listTimer = setTimeout(function () {
            listTimer = null;
            const list = roomList();
            connections.forEach(function (conn) {
                if (conn.hello && !conn.room) send(conn, { type: 'rooms', rooms: list });
            });
        }, 1000);
    }

    function broadcast(room, message, exceptPlayer) {
        room.players.forEach(function (p) {
            if (p !== exceptPlayer && p.conn) send(p.conn, message);
        });
    }

    function broadcastRoomNow(room) {
        if (room.broadcastTimer) {
            clearTimeout(room.broadcastTimer);
            room.broadcastTimer = null;
        }
        broadcast(room, { type: 'room', room: publicRoom(room) });
        broadcastListSoon();
    }

    function broadcastRoomSoon(room) {
        if (room.broadcastTimer) return;
        room.broadcastTimer = setTimeout(function () { broadcastRoomNow(room); }, 1000);
    }

    function systemChat(room, text) {
        const entry = { id: randomId(4), system: true, text: text, at: Date.now() };
        room.chat.push(entry);
        if (room.chat.length > CHAT_HISTORY) room.chat.shift();
        broadcast(room, { type: 'chat', entry: entry });
    }

    function pickNewHost(room) {
        const candidate = onlinePlayers(room)[0];
        room.hostId = candidate ? candidate.id : null;
        if (candidate) systemChat(room, candidate.profile.name + ' artık host.');
    }

    function deleteRoom(room, reason) {
        if (!rooms.has(room.id)) return;
        rooms.delete(room.id);
        if (room.broadcastTimer) clearTimeout(room.broadcastTimer);
        if (room.expireTimer) clearTimeout(room.expireTimer);
        room.players.forEach(function (p) {
            if (p.conn) {
                p.conn.room = null;
                p.conn.player = null;
                send(p.conn, { type: 'left', reason: reason || 'Oda kapandı.' });
                sendList(p.conn);
            }
        });
        log('oda silindi: ' + room.name + ' (' + room.id + ')');
        broadcastListSoon();
    }

    // Oda boşaldığında: lobideyse hemen sil, oyundaysa geri dönüş için bir süre tut.
    function checkEmpty(room) {
        if (onlinePlayers(room).length > 0) {
            if (room.expireTimer) {
                clearTimeout(room.expireTimer);
                room.expireTimer = null;
            }
            return;
        }
        if (room.phase === 'lobby') {
            deleteRoom(room);
        } else if (!room.expireTimer) {
            room.expireTimer = setTimeout(function () { deleteRoom(room); }, EMPTY_GAME_ROOM_TTL_MS);
        }
    }

    function detach(conn, reasonText) {
        const room = conn.room;
        const player = conn.player;
        conn.room = null;
        conn.player = null;
        if (!room || !player || player.conn !== conn) return;
        player.conn = null;
        player.ping = null;
        if (room.phase === 'lobby') room.players.delete(player.token);
        if (rooms.has(room.id)) {
            systemChat(room, player.profile.name + ' ' + reasonText);
            if (player.id === room.hostId) pickNewHost(room);
            broadcastRoomNow(room);
            checkEmpty(room);
        }
    }

    function attach(conn, room, player) {
        if (player.conn && player.conn !== conn) {
            const old = player.conn;
            old.room = null;
            old.player = null;
            send(old, { type: 'left', reason: 'Başka bir yerden bu odaya girildi.' });
        }
        player.conn = conn;
        conn.room = room;
        conn.player = player;
        const currentHost = playerById(room, room.hostId);
        if (!currentHost || !currentHost.conn) room.hostId = player.id;
        checkEmpty(room);
        send(conn, {
            type: 'joined',
            playerId: player.id,
            room: publicRoom(room),
            chat: room.chat,
            clock: room.lastClock
        });
        systemChat(room, player.profile.name + ' odaya katıldı. (' + onlinePlayers(room).length + '/' + room.maxPlayers + ')');
        broadcastRoomNow(room);
    }

    function newPlayer(conn) {
        return {
            id: randomId(4),
            token: conn.token,
            profile: conn.profile,
            ready: false,
            loaded: false,
            stats: null,
            ping: null,
            joinedAt: Date.now(),
            conn: null
        };
    }

    function findMembership(token) {
        for (const room of rooms.values()) {
            const player = room.players.get(token);
            if (player) return { room: room, player: player };
        }
        return null;
    }

    function fail(conn, code, message) {
        send(conn, { type: 'error', code: code, message: message });
    }

    // ------------------------------------------------------------ lobi mesajları

    function handleHello(conn, msg) {
        if (Number(msg.protocol) !== PROTOCOL_VERSION) {
            send(conn, { type: 'fatal', code: 'version', message: 'Mod sürümün sunucuyla uyuşmuyor. Modu güncelle.' });
            conn.ws.close();
            return;
        }
        conn.token = clean(msg.token, 64);
        if (conn.token.length < 8) {
            send(conn, { type: 'fatal', code: 'token', message: 'Geçersiz oyuncu kimliği.' });
            conn.ws.close();
            return;
        }
        conn.profile = sanitizeProfile(msg.profile);
        conn.hello = true;
        send(conn, { type: 'hello-ok', protocol: PROTOCOL_VERSION, serverTime: Date.now() });

        // Oyun sırasında kopan oyuncu: odasına otomatik geri al.
        const membership = findMembership(conn.token);
        if (membership && membership.room.phase === 'game') {
            membership.player.profile = conn.profile;
            membership.player.loaded = Boolean(membership.room.session && msg.loadedSessionId === membership.room.session.id);
            attach(conn, membership.room, membership.player);
            return;
        }
        sendList(conn);
    }

    function handleCreate(conn, msg) {
        if (conn.room) return fail(conn, 'in-room', 'Zaten bir odadasın.');
        if (rooms.size >= MAX_ROOMS) return fail(conn, 'busy', 'Sunucu dolu, biraz sonra tekrar dene.');
        const membership = findMembership(conn.token);
        if (membership) {
            membership.room.players.delete(conn.token);
            checkEmpty(membership.room);
        }

        const room = {
            id: randomId(4),
            name: clean(msg.name, 40) || (conn.profile.name + ' odası'),
            password: clean(msg.password, 40),
            maxPlayers: clampInt(msg.maxPlayers, 2, HARD_MAX_PLAYERS, HARD_MAX_PLAYERS),
            hostId: null,
            phase: 'lobby',
            settings: sanitizeSettings(msg.settings, DEFAULT_SETTINGS),
            session: null,
            lastClock: null,
            chat: [],
            players: new Map(),
            createdAt: Date.now(),
            broadcastTimer: null,
            expireTimer: null
        };

        // Kayıttan devam: aynı oturumun odası zaten açıksa ona yönlendir.
        if (msg.session && msg.session.id) {
            const sessionId = clean(msg.session.id, 64);
            for (const other of rooms.values()) {
                if (other.session && other.session.id === sessionId) {
                    return fail(conn, 'session-open', 'Bu kaydın odası zaten açık: "' + other.name + '". Listeden katıl.');
                }
            }
            room.phase = 'game';
            room.settings = sanitizeSettings(msg.session.settings, room.settings);
            room.session = {
                id: sessionId,
                startDateMs: finiteOrNull(msg.session.startDateMs) || Date.now(),
                settings: room.settings,
                startedAt: Date.now(),
                resumed: true
            };
        }

        rooms.set(room.id, room);
        const player = newPlayer(conn);
        player.loaded = Boolean(room.session && msg.loadedSessionId === room.session.id);
        room.players.set(conn.token, player);
        room.hostId = player.id;
        log('oda kuruldu: ' + room.name + ' (' + room.id + ') host=' + conn.profile.name);
        attach(conn, room, player);
    }

    function handleJoin(conn, msg) {
        if (conn.room && conn.room.id === clean(msg.roomId, 32)) return;
        if (conn.room) return fail(conn, 'in-room', 'Zaten bir odadasın.');
        const room = rooms.get(clean(msg.roomId, 32));
        if (!room) return fail(conn, 'not-found', 'Oda bulunamadı (kapanmış olabilir).');
        let player = room.players.get(conn.token);
        if (room.banned && room.banned.has(conn.token)) return fail(conn, 'banned', 'Bu odadan atıldın.');
        if (room.password && clean(msg.password, 40) !== room.password && !player) {
            return fail(conn, 'password', 'Oda şifresi yanlış.');
        }
        if (!player) {
            if (room.phase === 'game' && !room.settings.allowLateJoin) {
                return fail(conn, 'started', 'Oyun başladı ve host geç katılıma izin vermiyor.');
            }
            if (room.players.size >= room.maxPlayers) {
                return fail(conn, 'full', 'Oda dolu (' + room.maxPlayers + '/' + room.maxPlayers + ').');
            }
            const membership = findMembership(conn.token);
            if (membership) {
                membership.room.players.delete(conn.token);
                checkEmpty(membership.room);
            }
            player = newPlayer(conn);
            room.players.set(conn.token, player);
        }
        player.profile = conn.profile;
        player.loaded = Boolean(room.session && msg.loadedSessionId === room.session.id);
        attach(conn, room, player);
    }

    function handleLeave(conn) {
        const room = conn.room;
        const player = conn.player;
        if (!room || !player) return;
        // Bilerek ayrılan oyuncu slotunu bırakır.
        detach(conn, 'odadan ayrıldı.');
        room.players.delete(player.token);
        if (rooms.has(room.id)) {
            broadcastRoomNow(room);
            checkEmpty(room);
        }
        send(conn, { type: 'left', reason: '' });
        sendList(conn);
    }

    // ------------------------------------------------------------ oda içi mesajlar

    function isHost(room, player) {
        return player && room.hostId === player.id;
    }

    function handleRoomMessage(conn, msg) {
        const room = conn.room;
        const player = conn.player;
        if (!room || !player || player.conn !== conn) return;

        switch (msg.type) {
            case 'profile':
                if (room.phase !== 'lobby') return;
                conn.profile = player.profile = sanitizeProfile(msg.profile);
                broadcastRoomNow(room);
                break;

            case 'ready':
                player.ready = Boolean(msg.ready);
                broadcastRoomNow(room);
                break;

            case 'chat': {
                const text = clean(msg.text, 300);
                if (!text) return;
                const entry = { id: randomId(4), from: player.id, name: player.profile.name, text: text, at: Date.now() };
                room.chat.push(entry);
                if (room.chat.length > CHAT_HISTORY) room.chat.shift();
                broadcast(room, { type: 'chat', entry: entry });
                break;
            }

            case 'settings': {
                if (!isHost(room, player)) return;
                if (room.phase === 'lobby') {
                    room.settings = sanitizeSettings(msg.settings, room.settings);
                } else {
                    const allowed = {};
                    IN_GAME_SETTINGS.forEach(function (k) {
                        if (msg.settings && msg.settings[k] !== undefined) allowed[k] = msg.settings[k];
                    });
                    room.settings = sanitizeSettings(allowed, room.settings);
                    if (room.session) room.session.settings = room.settings;
                }
                if (msg.maxPlayers !== undefined) {
                    room.maxPlayers = clampInt(msg.maxPlayers, Math.max(2, room.players.size), HARD_MAX_PLAYERS, room.maxPlayers);
                }
                if (msg.name !== undefined) room.name = clean(msg.name, 40) || room.name;
                if (msg.password !== undefined) room.password = clean(msg.password, 40);
                broadcastRoomNow(room);
                break;
            }

            case 'kick': {
                if (!isHost(room, player)) return;
                const target = playerById(room, msg.playerId);
                if (!target || target === player) return;
                room.banned = room.banned || new Set();
                room.banned.add(target.token);
                room.players.delete(target.token);
                if (target.conn) {
                    const targetConn = target.conn;
                    target.conn = null;
                    targetConn.room = null;
                    targetConn.player = null;
                    send(targetConn, { type: 'left', reason: 'Host seni odadan attı.' });
                    sendList(targetConn);
                }
                systemChat(room, target.profile.name + ' odadan atıldı.');
                broadcastRoomNow(room);
                break;
            }

            case 'transfer-host': {
                if (!isHost(room, player)) return;
                const target = playerById(room, msg.playerId);
                if (!target || !target.conn) return;
                room.hostId = target.id;
                systemChat(room, target.profile.name + ' artık host.');
                broadcastRoomNow(room);
                break;
            }

            case 'start': {
                if (!isHost(room, player) || room.phase !== 'lobby') return;
                const start = new Date(finiteOrNull(msg.startDateMs) || Date.now());
                start.setHours(8, 0, 0, 0);
                room.phase = 'game';
                room.session = {
                    id: randomId(8),
                    startDateMs: start.getTime(),
                    settings: Object.assign({}, room.settings),
                    startedAt: Date.now(),
                    resumed: false
                };
                room.lastClock = null;
                players(room).forEach(function (p) {
                    p.loaded = false;
                    p.stats = null;
                    if (!p.conn) room.players.delete(p.token);
                });
                systemChat(room, 'Oyun başladı! Herkes kendi şirketini kuruyor…');
                broadcast(room, { type: 'session-start', session: room.session });
                broadcastRoomNow(room);
                log('oturum başladı: ' + room.name + ' / ' + room.session.id + ' (' + room.players.size + ' oyuncu)');
                break;
            }

            case 'end-session':
                if (!isHost(room, player) || room.phase !== 'game') return;
                room.phase = 'lobby';
                room.session = null;
                room.lastClock = null;
                players(room).forEach(function (p) {
                    p.ready = false;
                    p.loaded = false;
                    p.stats = null;
                    if (!p.conn) room.players.delete(p.token);
                });
                systemChat(room, 'Host oyunu bitirdi. Lobiye dönüldü.');
                broadcast(room, { type: 'session-end' });
                broadcastRoomNow(room);
                break;

            case 'loaded':
                if (!room.session || msg.sessionId !== room.session.id) return;
                if (!player.loaded) systemChat(room, player.profile.name + ' şirketini kurdu: ' + player.profile.companyName);
                player.loaded = true;
                broadcastRoomNow(room);
                break;

            case 'snapshot':
                if (!room.session || msg.sessionId !== room.session.id) return;
                player.stats = sanitizeStats(msg.stats);
                player.loaded = true;
                broadcastRoomSoon(room);
                break;

            case 'clock':
                if (!isHost(room, player) || !room.session) return;
                room.lastClock = {
                    sessionId: room.session.id,
                    dateMs: finiteOrNull(msg.dateMs),
                    paused: Boolean(msg.paused),
                    speed: clampInt(msg.speed, 1, 3, 1),
                    waiting: clean(msg.waiting, 160),
                    at: Date.now()
                };
                broadcast(room, { type: 'clock', clock: room.lastClock }, player);
                break;

            case 'clock-request': {
                if (!room.session || !room.settings.clientsCanPause) return;
                const host = playerById(room, room.hostId);
                if (!host || host === player || !host.conn) return;
                send(host.conn, {
                    type: 'clock-request',
                    from: player.id,
                    name: player.profile.name,
                    paused: Boolean(msg.paused),
                    speed: clampInt(msg.speed, 1, 3, 1)
                });
                break;
            }

            case 'money': {
                const txId = clean(msg.txId, 40);
                const target = playerById(room, msg.to);
                const amount = Math.floor(Number(msg.amount));
                let failure = null;
                if (!room.session || !room.settings.moneyTransfers) failure = 'Para transferi kapalı.';
                else if (!target || target === player) failure = 'Alıcı bulunamadı.';
                else if (!target.conn || !target.loaded) failure = target.profile.name + ' şu an oyunda değil.';
                else if (!isFinite(amount) || amount <= 0 || amount > 1e12) failure = 'Geçersiz miktar.';
                if (failure) {
                    send(conn, { type: 'money-result', ok: false, txId: txId, reason: failure });
                    return;
                }
                send(conn, { type: 'money-result', ok: true, txId: txId, amount: amount, to: target.id, toName: target.profile.name });
                send(target.conn, { type: 'money-in', txId: txId, amount: amount, from: player.id, fromName: player.profile.name });
                systemChat(room, player.profile.name + ' → ' + target.profile.name + ': $' + amount.toLocaleString('en-US'));
                break;
            }

            default:
                break;
        }
    }

    function handleMessage(conn, msg) {
        if (!msg || typeof msg.type !== 'string') return;
        if (msg.type === 'hello') {
            if (!conn.hello) handleHello(conn, msg);
            return;
        }
        if (!conn.hello) return;

        switch (msg.type) {
            case 'ping':
                if (conn.player) conn.player.ping = finiteOrNull(msg.lastPing) === null ? null : clampInt(msg.lastPing, 0, 99999, null);
                send(conn, { type: 'pong', t: msg.t });
                break;
            case 'list':
                sendList(conn);
                break;
            case 'set-profile':
                conn.profile = sanitizeProfile(msg.profile);
                if (conn.player && conn.room && conn.room.phase === 'lobby') {
                    conn.player.profile = conn.profile;
                    broadcastRoomNow(conn.room);
                }
                break;
            case 'create-room':
                handleCreate(conn, msg);
                break;
            case 'join-room':
                handleJoin(conn, msg);
                break;
            case 'leave-room':
                handleLeave(conn);
                break;
            default:
                handleRoomMessage(conn, msg);
        }
    }

    // ------------------------------------------------------------ HTTP + WebSocket

    const httpServer = http.createServer(function (req, res) {
        if (req.url === '/health' || req.url.indexOf('/health?') === 0) {
            let playerCount = 0;
            rooms.forEach(function (room) { playerCount += onlinePlayers(room).length; });
            res.writeHead(200, { 'content-type': 'application/json', 'access-control-allow-origin': '*', 'cache-control': 'no-store' });
            res.end(JSON.stringify({ ok: true, protocol: PROTOCOL_VERSION, rooms: rooms.size, players: playerCount, connections: connections.size }));
            return;
        }
        res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
        res.end('Startup Company Multiplayer 16 sunucusu çalışıyor.\n');
    });

    const wss = new WebSocketServer({ server: httpServer, maxPayload: MAX_MESSAGE_BYTES });

    wss.on('connection', function (ws, req) {
        const forwarded = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim();
        const ip = forwarded || req.socket.remoteAddress || '?';
        const count = (ipCounts.get(ip) || 0) + 1;
        if (count > MAX_CONNECTIONS_PER_IP) {
            ws.close(1008, 'Çok fazla bağlantı');
            return;
        }
        ipCounts.set(ip, count);

        const conn = { ws: ws, ip: ip, hello: false, token: '', profile: null, room: null, player: null, alive: true, rate: { start: Date.now(), count: 0 } };
        connections.add(conn);

        ws.on('pong', function () { conn.alive = true; });
        ws.on('message', function (data) {
            conn.alive = true;
            const now = Date.now();
            if (now - conn.rate.start > RATE_WINDOW_MS) conn.rate = { start: now, count: 0 };
            conn.rate.count += 1;
            if (conn.rate.count > RATE_HARD_LIMIT) {
                ws.close(1008, 'Çok fazla mesaj');
                return;
            }
            if (conn.rate.count > RATE_SOFT_LIMIT) return;
            let msg;
            try {
                msg = JSON.parse(String(data));
            } catch (_) {
                return;
            }
            try {
                handleMessage(conn, msg);
            } catch (error) {
                log('mesaj hatası (' + msg.type + '): ' + (error && error.stack || error));
            }
        });
        ws.on('error', function () {});
        ws.on('close', function () {
            connections.delete(conn);
            const left = (ipCounts.get(ip) || 1) - 1;
            if (left <= 0) ipCounts.delete(ip); else ipCounts.set(ip, left);
            detach(conn, 'bağlantısı koptu.');
        });
    });

    // Ölü bağlantıları temizle.
    const heartbeat = setInterval(function () {
        connections.forEach(function (conn) {
            if (!conn.alive) {
                conn.ws.terminate();
                return;
            }
            conn.alive = false;
            try {
                conn.ws.ping();
            } catch (_) {}
        });
    }, 30000);

    return {
        rooms: rooms,
        httpServer: httpServer,
        listen: function (port, callback) {
            httpServer.once('error', function (error) { callback(error); });
            httpServer.listen(port, function () { callback(null, httpServer.address()); });
        },
        close: function (callback) {
            clearInterval(heartbeat);
            if (listTimer) clearTimeout(listTimer);
            rooms.forEach(function (room) { deleteRoom(room, 'Sunucu kapanıyor.'); });
            connections.forEach(function (conn) { conn.ws.terminate(); });
            wss.close();
            httpServer.close(function () { if (callback) callback(); });
        }
    };
}

module.exports = {
    createLobbyServer: createLobbyServer,
    PROTOCOL_VERSION: PROTOCOL_VERSION,
    HARD_MAX_PLAYERS: HARD_MAX_PLAYERS
};

if (require.main === module) {
    const port = clampInt(process.env.PORT || process.argv[2] || 8080, 1, 65535, 8080);
    const instance = createLobbyServer({
        log: function (text) { console.log(new Date().toISOString() + ' | ' + text); }
    });
    instance.listen(port, function (error, address) {
        if (error) {
            console.error('Sunucu başlatılamadı: ' + error.message);
            process.exit(1);
        }
        console.log('Startup Company Multiplayer 16 sunucusu ' + address.port + ' portunda çalışıyor.');
    });
    const shutdown = function () { instance.close(function () { process.exit(0); }); };
    process.on('SIGINT', shutdown);
    process.on('SIGTERM', shutdown);
}
