/* Futures Executor — dashboard en tiempo real (WebSocket, sin dependencias). */
(() => {
  'use strict';

  // ── Utilidades ─────────────────────────────────────────────────────────
  const $ = (s, r = document) => r.querySelector(s);
  const $$ = (s, r = document) => [...r.querySelectorAll(s)];
  const store = {
    get(k, d) { try { const v = localStorage.getItem('fx.' + k); return v === null ? d : JSON.parse(v); } catch { return d; } },
    set(k, v) { try { localStorage.setItem('fx.' + k, JSON.stringify(v)); } catch { /* almacenamiento no disponible */ } },
    del(k) { try { localStorage.removeItem('fx.' + k); } catch { /* idem */ } },
  };
  const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const dcls = (d) => (d === 'LONG' ? 'long' : d === 'SHORT' ? 'short' : '');  // clase CSS segura
  const num = (v) => (Number.isFinite(+v) ? +v : 0);
  const debounce = (fn, ms) => { let t; return (...a) => { clearTimeout(t); t = setTimeout(() => fn(...a), ms); }; };

  function magDec(p) {
    p = Math.abs(num(p));
    if (p >= 1000) return 2; if (p >= 10) return 3; if (p >= 1) return 4; if (p >= 0.1) return 5; if (p >= 0.01) return 6;
    return 8;
  }
  function priceDec(sym, p) { const r = S.rules[sym]; return r ? Math.max(r.price_decimals, 0) : magDec(p); }
  function fp(p, sym) {
    if (!num(p)) return '–';
    const d = sym ? priceDec(sym, p) : magDec(p);
    return num(p).toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d });
  }
  function fn(v, d = 2) { return num(v).toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d }); }
  function fq(v) { return num(v).toLocaleString('en-US', { maximumFractionDigits: 8 }); }
  function sg(v, d = 2) { v = num(v); return (v > 0 ? '+' : '') + fn(v, d); }
  function cl(v) { v = num(v); return v > 0 ? 'up' : v < 0 ? 'down' : ''; }
  function compact(n) {
    n = num(n);
    if (n >= 1e9) return fn(n / 1e9, 2) + 'B'; if (n >= 1e6) return fn(n / 1e6, 2) + 'M'; if (n >= 1e3) return fn(n / 1e3, 1) + 'K';
    return fn(n, 2);
  }
  function dur(s) {
    s = Math.max(0, Math.floor(num(s)));
    const d = Math.floor(s / 86400), h = Math.floor((s % 86400) / 3600), m = Math.floor((s % 3600) / 60);
    if (d) return `${d}d ${h}h`; if (h) return `${h}h ${m}m`; if (m) return `${m}m ${s % 60}s`; return `${s}s`;
  }
  function hms(ts) { return new Date(ts * 1000).toLocaleTimeString('es', { hour12: false }); }
  function dt(ts) { const d = new Date(ts * 1000); return d.toLocaleDateString('es', { day: '2-digit', month: '2-digit' }) + ' ' + hms(ts); }
  const baseOf = (sym) => sym.replace(/(USDT|USDC)$/, '');

  // ── Estado ─────────────────────────────────────────────────────────────
  const S = {
    snap: null, markets: [], marketMap: new Map(), ticker: null,
    symbol: store.get('symbol', 'BTCUSDT'), rules: {},
    favs: new Set(store.get('favs', [])), sort: store.get('sort', 'vol'), search: '',
    tab: store.get('tab', 'positions'), tradeTab: 'open', ordType: 'MARKET', gridMode: 'NEUTRAL',
    openLev: 0, errors: [], logs: [], catalog: null, onlySym: false, tpslAuto: null,
    gridSymbol: '', gridDetail: null,
  };

  // ── WebSocket ──────────────────────────────────────────────────────────
  const WS = {
    sock: null, seq: 0, pending: new Map(), retry: 0, token: '', authed: false, denied: false,
    connect() {
      const proto = location.protocol === 'https:' ? 'wss' : 'ws';
      const sock = new WebSocket(`${proto}://${location.host}/ws`);
      this.sock = sock;
      setDot('c-dash', 'warn');
      sock.onopen = () => { this.retry = 0; this.send({ op: 'auth', token: this.token }); };
      sock.onmessage = (e) => { try { onMessage(JSON.parse(e.data)); } catch (err) { console.error(err); } };
      sock.onclose = () => {
        this.authed = false;
        setDot('c-dash', 'off');
        for (const p of this.pending.values()) { clearTimeout(p.t); p.rej(new Error('Conexión con el panel perdida')); }
        this.pending.clear();
        if (!this.denied) {
          if (!WS.everAuthed) $('#connecting-msg').textContent = 'Sin conexión con el executor; reintentando…';
          setTimeout(() => this.connect(), Math.min(10000, 500 * 2 ** this.retry++));
        }
      };
    },
    send(o) { if (this.sock && this.sock.readyState === 1) this.sock.send(JSON.stringify(o)); },
    cmd(cmd, args = {}, timeout = 45000) {
      return new Promise((res, rej) => {
        if (!this.authed) { rej(new Error('Sin conexión con el executor')); return; }
        const id = ++this.seq;
        const t = setTimeout(() => { this.pending.delete(id); rej(new Error('Tiempo de espera agotado')); }, timeout);
        this.pending.set(id, { res, rej, t });
        this.send({ op: 'cmd', id, cmd, args });
      });
    },
  };

  async function run(cmd, args, okMsg) {
    try {
      const data = await WS.cmd(cmd, args);
      if (okMsg) toast('success', typeof okMsg === 'function' ? okMsg(data) : okMsg);
      return data;
    } catch (e) {
      toastError(e);
      return null;
    }
  }

  function onMessage(m) {
    switch (m.type) {
      case 'auth':
        if (m.ok) {
          WS.authed = true;
          WS.everAuthed = true;
          WS.denied = false;
          setDot('c-dash', 'on');
          $('#login').hidden = true;
          $('#app').hidden = false;
          $('#login-error').textContent = '';
          $('#logout-btn').hidden = !m.required;
          WS.send({ op: 'markets', on: true });
          selectSymbol(S.symbol, true);
        } else {
          // Solo ocurre si el servidor tiene DASHBOARD_TOKEN configurado.
          const hadToken = !!WS.token;
          WS.denied = true;
          WS.token = '';
          store.del('token');
          $('#login').hidden = false;
          $('#connecting').hidden = true;
          $('#login-form').hidden = false;
          $('#app').hidden = true;
          $('#login-error').textContent = hadToken ? (m.error || 'Token inválido') : '';
          $('#login-token').focus();
        }
        break;
      case 'state': S.snap = m.data; renderAll(); break;
      case 'markets':
        S.markets = m.rows;
        S.marketMap = new Map(m.rows.map((r) => [r[0], r]));
        renderMarkets();
        break;
      case 'ticker':
        if (m.data.symbol === S.symbol) {
          S.ticker = m.data;
          S.rules[m.data.symbol] = m.data.rules;
          renderSymbol();
          if (m.data.mark) Chart.push(Date.now(), m.data.mark);
        }
        break;
      case 'event': {
        const ev = m.event;
        if (['success', 'warning', 'error'].includes(ev.level) || ev.kind === 'grid') toast(ev.level, ev.text, ev.data && ev.data.solution);
        break;
      }
      case 'error': {
        const i = S.errors.findIndex((e) => e.id === m.entry.id);
        if (i >= 0) S.errors[i] = m.entry; else S.errors.unshift(m.entry);
        S.errors = S.errors.slice(0, 300);
        if (i < 0 && m.entry.severity === 'critical') toast('error', `[${m.entry.code || m.entry.http_status}] ${m.entry.title}`, m.entry.solution);
        renderErrors();
        break;
      }
      case 'errors': S.errors = m.entries; renderErrors(); break;
      case 'logs':
        S.logs.push(...m.lines);
        if (S.logs.length > 400) S.logs = S.logs.slice(-400);
        if (S.tab === 'system') renderLogs();
        break;
      case 'reply': {
        const p = WS.pending.get(m.id);
        if (!p) break;
        WS.pending.delete(m.id);
        clearTimeout(p.t);
        if (m.ok) p.res(m.data);
        else p.rej(Object.assign(new Error(m.error || 'Error'), { diagnosis: m.diagnosis }));
        break;
      }
      default: break;
    }
  }

  // ── Toasts y modales ───────────────────────────────────────────────────
  const TITLES = { success: 'Listo', error: 'Error', warning: 'Atención', info: 'Info' };
  function toast(level, text, solution) {
    const box = $('#toasts');
    const el = document.createElement('div');
    el.className = `toast ${level || 'info'}`;
    el.innerHTML = `<b>${esc(TITLES[level] || 'Info')}</b>${esc(text)}${solution ? `<div class="sol">💡 ${esc(solution)}</div>` : ''}`;
    box.appendChild(el);
    while (box.children.length > 5) box.firstChild.remove();
    setTimeout(() => el.remove(), level === 'error' ? 9000 : 5000);
  }
  function toastError(e) {
    const d = e.diagnosis;
    toast('error', e.message, d ? d.solution : '');
  }

  let modalClose = null;
  function openModal({ title, body, buttons = [], wide = false, onOpen }) {
    $('#modal-title').textContent = title;
    $('#modal-body').innerHTML = body;
    const foot = $('#modal-foot');
    foot.innerHTML = '';
    for (const b of buttons) {
      const el = document.createElement('button');
      el.className = `btn ${b.cls || 'btn-ghost'}`;
      el.textContent = b.label;
      el.type = 'button';
      el.onclick = () => b.onClick && b.onClick(el);
      foot.appendChild(el);
    }
    $('.modal-box').classList.toggle('wide', wide);
    $('#modal').hidden = false;
    modalClose = () => { $('#modal').hidden = true; modalClose = null; S.gridDetail = null; };
    if (onOpen) onOpen($('#modal-body'));
    return modalClose;
  }
  function closeModal() { if (modalClose) modalClose(); }
  function confirmBox(title, html, okLabel = 'Confirmar', cls = 'btn-primary') {
    return new Promise((resolve) => {
      openModal({
        title, body: html,
        buttons: [
          { label: 'Cancelar', onClick: () => { closeModal(); resolve(false); } },
          { label: okLabel, cls, onClick: () => { closeModal(); resolve(true); } },
        ],
      });
    });
  }

  // ── Render general ─────────────────────────────────────────────────────
  function setDot(id, state) { const el = document.getElementById(id); if (el) el.className = `dot ${state}`; }

  function renderAll() {
    const s = S.snap;
    if (!s) return;
    renderHeader(s);
    renderCards(s);
    renderCounts(s);
    renderEstimates();
    if (S.tab === 'positions') renderPositions();
    if (S.tab === 'orders') renderOrders();
    if (S.tab === 'grids') renderGrids();
    if (S.tab === 'history') renderHistory();
    if (S.tab === 'signals') renderSignals();
    if (S.tab === 'system') renderSystem();
    if (S.gridDetail) refreshGridDetail();
    Chart.requestDraw();
  }

  function renderHeader(s) {
    const pill = $('#env-pill');
    pill.textContent = s.env;
    pill.className = `pill ${s.env === 'REAL' ? 'real' : 'testnet'}`;
    const h = s.health;
    setDot('c-api', h.ws_api.connected ? 'on' : (h.credentials ? 'off' : ''));
    $('#c-lat').textContent = h.ws_api.latency_ms ? `${Math.round(h.ws_api.latency_ms)} ms` : '–';
    setDot('c-mkt', h.market.connected ? 'on' : 'off');
    setDot('c-usr', h.user.connected ? 'on' : (h.credentials ? 'off' : ''));
    $('#trading-toggle').checked = !!s.trading_enabled;
    $('#lev-btn').textContent = `${s.leverage}x`;
    const ex = h.exchange_info;
    $('#foot').innerHTML = [
      `v${esc(s.version)}`, `activo ${dur(s.uptime_s)}`,
      `exchangeInfo: ${ex.count ? `${ex.count} símbolos` : '<span class="text-yellow">sin snapshot</span>'}`,
      `REST usadas: ${h.rest.total}`, `WS API: ${h.ws_api.requests} peticiones`,
      `reloj ${sg(h.clock_offset_ms, 0)} ms`,
    ].map((x) => `<span>${x}</span>`).join('');
  }

  function renderCards(s) {
    const a = s.account, st = s.stats;
    $('#k-margin').innerHTML = `${fn(a.margin_balance)}<small>USDT</small>`;
    $('#k-wallet').textContent = `Billetera ${fn(a.wallet)}`;
    $('#k-avail').innerHTML = `${fn(a.available)}<small>USDT</small>`;
    $('#k-mode').textContent = `Modo ${a.hedge_mode ? 'Hedge' : 'One-way'}${a.hedge_detected ? '' : ' (supuesto)'}`;
    const up = $('#k-upnl');
    up.innerHTML = `${sg(a.unrealized, 4)}<small>USDT</small>`;
    up.className = cl(a.unrealized);
    $('#k-pos').textContent = `${a.positions} posiciones · ${a.orders + a.algos} órdenes`;
    const rp = $('#k-rpnl');
    rp.innerHTML = `${sg(st.realized_pnl, 4)}<small>USDT</small>`;
    rp.className = cl(st.realized_pnl);
    $('#k-fees').textContent = `Comisiones ${fn(st.fees, 4)}`;
    $('#k-wr').textContent = st.win_rate == null ? 'N/A' : `${fn(st.win_rate, 1)}%`;
    $('#k-wl').textContent = `${st.wins} ganadas · ${st.losses} perdidas`;
    const active = s.grids.filter((g) => ['RUNNING', 'PENDING', 'STARTING'].includes(g.status));
    const gp = s.grids.reduce((acc, g) => acc + num(g.total_pnl), 0);
    $('#k-grids').textContent = `${active.length} activos`;
    const gs = $('#k-gridpnl');
    gs.textContent = `PnL grids ${sg(gp, 4)} USDT`;
  }

  function renderCounts(s) {
    $('#n-positions').textContent = s.positions.length;
    $('#n-orders').textContent = s.orders.length;
    $('#n-grids').textContent = s.grids.filter((g) => g.status !== 'STOPPED').length;
    const ne = $('#n-errors');
    ne.textContent = S.errors.length || s.errors_total;
  }

  // ── Mercados ───────────────────────────────────────────────────────────
  function positionSymbols() { return new Set((S.snap ? S.snap.positions : []).map((p) => p.symbol)); }

  function renderMarkets() {
    const list = $('#mkt-list');
    if (!S.markets.length) return;
    const q = S.search.trim().toUpperCase();
    let rows = S.markets;
    if (q) rows = rows.filter((r) => r[0].includes(q));
    if (S.sort === 'fav') rows = rows.filter((r) => S.favs.has(r[0]));
    const sorters = {
      vol: (a, b) => b[3] - a[3], fav: (a, b) => b[3] - a[3], chg: (a, b) => b[2] - a[2], name: (a, b) => a[0].localeCompare(b[0]),
    };
    rows = rows.slice().sort(sorters[S.sort] || sorters.vol).slice(0, q ? 200 : 400);
    const held = positionSymbols();
    if (!rows.length) { list.innerHTML = `<div class="empty">${S.sort === 'fav' ? 'Marca favoritos con ★' : 'Sin resultados'}</div>`; return; }
    list.innerHTML = rows.map(([sym, last, chg]) => `
      <div class="mkt-row${sym === S.symbol ? ' sel' : ''}" data-sym="${sym}">
        <span class="sym"><b class="star${S.favs.has(sym) ? ' on' : ''}" data-star="${sym}">★</b>${esc(baseOf(sym))}<small class="muted">/${sym.endsWith('USDC') ? 'USDC' : 'USDT'}</small>${held.has(sym) ? '<i class="has"></i>' : ''}</span>
        <span class="${cl(chg)}">${fp(last)}</span><span class="${cl(chg)}">${sg(chg)}%</span>
      </div>`).join('');
  }

  function toggleFav(sym) {
    if (S.favs.has(sym)) S.favs.delete(sym); else S.favs.add(sym);
    store.set('favs', [...S.favs]);
    renderMarkets();
    $('#sym-fav').classList.toggle('on', S.favs.has(S.symbol));
    $('#sym-fav').textContent = S.favs.has(S.symbol) ? '★' : '☆';
  }

  async function selectSymbol(sym, initial = false) {
    sym = (sym || '').toUpperCase();
    if (!sym) return;
    const changed = sym !== S.symbol || initial;
    S.symbol = sym;
    store.set('symbol', sym);
    S.ticker = null;
    WS.send({ op: 'select', symbol: sym });
    $('#sym-name').textContent = sym;
    $('#sym-fav').classList.toggle('on', S.favs.has(sym));
    $('#sym-fav').textContent = S.favs.has(sym) ? '★' : '☆';
    renderMarkets();
    if (changed) {
      Chart.set([]);
      S.openLev = 0;
      const hist = await run('price_history', { symbol: sym });
      if (hist && hist.symbol === S.symbol) Chart.set(hist.points.map(([t, p]) => [t * 1000, p]));
      if (S.gridSymbol !== sym) { S.gridSymbol = sym; setGridRange(10); }
    }
    if (S.tab === 'positions') renderPositions();
  }

  // ── Barra de símbolo ───────────────────────────────────────────────────
  function renderSymbol() {
    const t = S.ticker;
    if (!t) return;
    const sym = t.symbol;
    const last = t.last || t.mark;
    const el = $('#sym-last');
    el.textContent = fp(last, sym);
    el.className = `big ${cl(t.change_pct)}`;
    $('#sym-mark-usd').textContent = `≈ ${fn(last, last >= 1 ? 2 : 6)} USD`;
    const chg = $('#sym-chg');
    chg.textContent = `${sg(t.last - t.open, priceDec(sym, last))} ${sg(t.change_pct)}%`;
    chg.className = cl(t.change_pct);
    $('#sym-mark').textContent = fp(t.mark, sym);
    $('#sym-index').textContent = fp(t.index, sym);
    const left = t.next_funding_ms ? Math.max(0, t.next_funding_ms - Date.now()) / 1000 : 0;
    const fr = $('#sym-funding');
    fr.innerHTML = `<span class="${t.funding_rate > 0 ? 'text-yellow' : cl(t.funding_rate)}">${fn(t.funding_rate * 100, 4)}%</span> / ${new Date(left * 1000).toISOString().substr(11, 8)}`;
    $('#sym-high').textContent = fp(t.high, sym);
    $('#sym-low').textContent = fp(t.low, sym);
    $('#sym-vol').textContent = compact(t.quote_volume);
    const r = t.rules;
    $('#sym-rules').innerHTML = r ? `${esc(r.tick)} · paso ${esc(r.step)} · mín ${esc(r.min_notional)} USDT${r.max_leverage ? ` · ${r.max_leverage}x máx` : ''}${r.source !== 'snapshot' ? ' <span class="badge y" title="Símbolo fuera del exchangeInfo local">aprox.</span>' : ''}` : '–';
    document.title = `${fp(last, sym)} ${sym} | Executor`;
    if (!S.openLev) S.openLev = t.leverage || (S.snap ? S.snap.leverage : 5);
    $('#open-lev').textContent = `${S.openLev}x`;
    if (!$('#g-lower').value && last) setGridRange(10);
    renderEstimates();
  }

  // ── Gráfico (canvas) ───────────────────────────────────────────────────
  const Chart = {
    pts: [], hover: null, pending: false,
    init() {
      this.cv = $('#chart');
      this.ctx = this.cv.getContext('2d');
      new ResizeObserver(() => this.requestDraw()).observe(this.cv.parentElement);
      this.cv.addEventListener('mousemove', (e) => { const r = this.cv.getBoundingClientRect(); this.hover = { x: e.clientX - r.left, y: e.clientY - r.top }; this.requestDraw(); });
      this.cv.addEventListener('mouseleave', () => { this.hover = null; $('#chart-tip').hidden = true; this.requestDraw(); });
    },
    set(points) { this.pts = points.slice(-1800); this.requestDraw(); },
    push(t, p) {
      const last = this.pts[this.pts.length - 1];
      if (!last || t - last[0] >= 900) this.pts.push([t, p]); else last[1] = p;
      if (this.pts.length > 1800) this.pts.shift();
      this.requestDraw();
    },
    requestDraw() { if (this.pending) return; this.pending = true; requestAnimationFrame(() => { this.pending = false; this.draw(); }); },
    overlays() {
      const out = [];
      const snap = S.snap;
      if (!snap) return out;
      for (const p of snap.positions.filter((x) => x.symbol === S.symbol)) {
        out.push({ price: p.entry, color: '#f0b90b', label: `Entrada ${p.direction}` });
        if (p.tp) out.push({ price: p.tp, color: '#0ecb81', label: 'TP' });
        if (p.sl) out.push({ price: p.sl, color: '#f6465d', label: 'SL' });
        if (p.liq) out.push({ price: p.liq, color: '#f7a600', label: 'Liq.' });
      }
      for (const g of snap.grids.filter((x) => x.symbol === S.symbol && x.status !== 'STOPPED')) {
        const n = g.grids, lo = g.lower, hi = g.upper;
        for (let k = 0; k <= n; k++) {
          const price = g.spacing === 'GEOMETRIC' ? lo * (hi / lo) ** (k / n) : lo + ((hi - lo) * k) / n;
          out.push({ price, color: 'rgba(60,143,232,.55)', label: k === 0 || k === n ? `Grid ${k === 0 ? 'inf.' : 'sup.'}` : '', faint: k !== 0 && k !== n });
        }
      }
      return out;
    },
    draw() {
      const cv = this.cv, ctx = this.ctx;
      if (!cv) return;
      const dpr = window.devicePixelRatio || 1;
      const w = cv.clientWidth, h = cv.clientHeight;
      if (!w || !h) return;
      if (cv.width !== Math.round(w * dpr) || cv.height !== Math.round(h * dpr)) { cv.width = Math.round(w * dpr); cv.height = Math.round(h * dpr); }
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, w, h);
      const css = getComputedStyle(document.documentElement);
      const c = (v) => css.getPropertyValue(v).trim();
      const pts = this.pts;
      const legend = $('#chart-legend');
      if (pts.length < 2) {
        ctx.fillStyle = c('--text-3');
        ctx.font = '12px "IBM Plex Sans", sans-serif';
        ctx.textAlign = 'center';
        ctx.fillText('Recopilando precios en tiempo real…', w / 2, h / 2);
        legend.innerHTML = '';
        return;
      }
      const padR = 70, padB = 18, padT = 8;
      let lo = Infinity, hi = -Infinity;
      for (const [, p] of pts) { if (p < lo) lo = p; if (p > hi) hi = p; }
      const span0 = Math.max(hi - lo, hi * 0.002);
      const ov = this.overlays().filter((o) => o.price >= lo - span0 * 1.5 && o.price <= hi + span0 * 1.5);
      for (const o of ov) { if (o.price < lo) lo = o.price; if (o.price > hi) hi = o.price; }
      const span = Math.max(hi - lo, hi * 0.002);
      lo -= span * 0.08; hi += span * 0.08;
      const t0 = pts[0][0], t1 = pts[pts.length - 1][0];
      const X = (t) => ((t - t0) / Math.max(1, t1 - t0)) * (w - padR);
      const Y = (p) => padT + (1 - (p - lo) / (hi - lo)) * (h - padT - padB);
      const dec = priceDec(S.symbol, hi);
      // Rejilla
      ctx.strokeStyle = c('--grid-line');
      ctx.fillStyle = c('--text-3');
      ctx.font = '11px "IBM Plex Sans", sans-serif';
      ctx.lineWidth = 1;
      ctx.textAlign = 'left';
      for (let i = 0; i <= 4; i++) {
        const p = lo + ((hi - lo) * i) / 4, y = Math.round(Y(p)) + 0.5;
        ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(w - padR, y); ctx.stroke();
        ctx.fillText(p.toFixed(dec), w - padR + 6, y + 4);
      }
      ctx.textAlign = 'center';
      const tf = t1 - t0 < 600000 ? { hour12: false } : { hour12: false, hour: '2-digit', minute: '2-digit' };
      for (let i = 1; i <= 3; i++) {
        const t = t0 + ((t1 - t0) * i) / 4;
        ctx.fillText(new Date(t).toLocaleTimeString('es', tf), X(t), h - 4);
      }
      // Overlays
      const legendItems = new Map();
      for (const o of ov) {
        const y = Math.round(Y(o.price)) + 0.5;
        ctx.strokeStyle = o.color;
        ctx.globalAlpha = o.faint ? 0.35 : 0.9;
        ctx.setLineDash(o.faint ? [2, 4] : [6, 4]);
        ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(w - padR, y); ctx.stroke();
        ctx.setLineDash([]);
        ctx.globalAlpha = 1;
        if (o.label) {
          ctx.fillStyle = o.color;
          ctx.textAlign = 'left';
          ctx.fillText(o.label, 6, y - 4);
          legendItems.set(o.label.split(' ')[0], o.color);
        }
      }
      // Línea y área
      const up = pts[pts.length - 1][1] >= pts[0][1];
      const col = up ? c('--green') : c('--red');
      const grad = ctx.createLinearGradient(0, padT, 0, h - padB);
      grad.addColorStop(0, up ? 'rgba(14,203,129,.22)' : 'rgba(246,70,93,.22)');
      grad.addColorStop(1, 'rgba(0,0,0,0)');
      ctx.beginPath();
      pts.forEach(([t, p], i) => (i ? ctx.lineTo(X(t), Y(p)) : ctx.moveTo(X(t), Y(p))));
      ctx.lineTo(X(t1), h - padB); ctx.lineTo(X(t0), h - padB); ctx.closePath();
      ctx.fillStyle = grad; ctx.fill();
      ctx.beginPath();
      pts.forEach(([t, p], i) => (i ? ctx.lineTo(X(t), Y(p)) : ctx.moveTo(X(t), Y(p))));
      ctx.strokeStyle = col; ctx.lineWidth = 1.6; ctx.stroke();
      // Precio actual
      const lp = pts[pts.length - 1][1], ly = Y(lp);
      ctx.fillStyle = col;
      ctx.fillRect(w - padR + 2, ly - 9, padR - 4, 18);
      ctx.fillStyle = '#fff';
      ctx.textAlign = 'left';
      ctx.fillText(lp.toFixed(dec), w - padR + 6, ly + 4);
      // Cursor
      const tip = $('#chart-tip');
      if (this.hover && this.hover.x < w - padR) {
        const tt = t0 + (this.hover.x / (w - padR)) * (t1 - t0);
        let best = pts[0];
        for (const p of pts) if (Math.abs(p[0] - tt) < Math.abs(best[0] - tt)) best = p;
        const x = X(best[0]), y = Y(best[1]);
        ctx.strokeStyle = c('--text-3'); ctx.setLineDash([3, 3]);
        ctx.beginPath(); ctx.moveTo(x, padT); ctx.lineTo(x, h - padB); ctx.moveTo(0, y); ctx.lineTo(w - padR, y); ctx.stroke();
        ctx.setLineDash([]);
        tip.hidden = false;
        tip.textContent = `${new Date(best[0]).toLocaleTimeString('es', { hour12: false })} · ${best[1].toFixed(dec)}`;
        tip.style.left = `${Math.min(x + 10, w - padR - tip.offsetWidth - 4)}px`;
        tip.style.top = `${Math.max(4, y - 30)}px`;
      } else tip.hidden = true;
      legend.innerHTML = [...legendItems].map(([k, v]) => `<span><i style="background:${v}"></i>${esc(k)}</span>`).join('');
    },
  };

  // ── Panel de apertura ──────────────────────────────────────────────────
  function refPrice() {
    if (S.ordType === 'LIMIT' && num($('#open-price').value) > 0) return num($('#open-price').value);
    return S.ticker ? (S.ticker.last || S.ticker.mark) : 0;
  }
  function renderEstimates() {
    if (!S.snap) return;
    const avail = S.snap.account.available;
    const lev = S.openLev || 1, price = refPrice();
    const amount = num($('#open-amount').value), mode = $('#open-mode').value;
    const notional = mode === 'margin' ? amount * lev : mode === 'qty' ? amount * price : amount;
    $('#est-avail').textContent = `${fn(avail)} USDT`;
    $('#est-notional').textContent = amount ? `${fn(notional)} USDT` : '–';
    $('#est-margin').textContent = amount ? `${fn(notional / lev)} USDT` : '–';
    $('#est-qty').textContent = amount && price ? `${fq(notional / price)} ${baseOf(S.symbol)}` : '–';
  }

  function applyRoe(roe) {
    const p = refPrice(), lev = S.openLev || 1;
    if (!p) return;
    const move = roe / 100 / lev;
    const d = priceDec(S.symbol, p);
    $('#open-tp').value = (p * (1 + move)).toFixed(d);
    $('#open-sl').value = (p * (1 - move)).toFixed(d);
    S.tpslAuto = roe;
  }

  async function submitOpen(direction) {
    const amount = num($('#open-amount').value);
    if (amount <= 0) { toast('warning', 'Indica un tamaño mayor que 0'); return; }
    const args = {
      symbol: S.symbol, direction, amount, size_mode: $('#open-mode').value, leverage: S.openLev,
      order_type: S.ordType,
    };
    if (S.ordType === 'LIMIT') {
      args.price = num($('#open-price').value);
      if (!args.price) { toast('warning', 'Indica el precio límite'); return; }
    }
    if ($('#open-tpsl').checked) {
      let tp = num($('#open-tp').value), sl = num($('#open-sl').value);
      if (S.tpslAuto && direction === 'SHORT') {
        const p = refPrice(), move = S.tpslAuto / 100 / (S.openLev || 1), d = priceDec(S.symbol, p);
        tp = +(p * (1 - move)).toFixed(d); sl = +(p * (1 + move)).toFixed(d);
      }
      if (tp) args.tp = tp;
      if (sl) args.sl = sl;
    }
    const sideTxt = direction === 'LONG' ? '<b class="up">Comprar / Long</b>' : '<b class="down">Vender / Short</b>';
    const ok = await confirmBox('Confirmar orden', `
      <div class="kv-list">
        <div><span>Operación</span>${sideTxt}</div>
        <div><span>Símbolo</span><b>${esc(S.symbol)}</b></div>
        <div><span>Tipo</span><b>${S.ordType === 'MARKET' ? 'Mercado' : `Límite @ ${fp(args.price, S.symbol)}`}</b></div>
        <div><span>Tamaño</span><b>${fn(amount, 4)} ${args.size_mode === 'qty' ? baseOf(S.symbol) : args.size_mode === 'margin' ? 'USDT de margen' : 'USDT'}</b></div>
        <div><span>Leverage</span><b>${S.openLev}x</b></div>
        ${args.tp ? `<div><span>Take profit</span><b class="up">${fp(args.tp, S.symbol)}</b></div>` : ''}
        ${args.sl ? `<div><span>Stop loss</span><b class="down">${fp(args.sl, S.symbol)}</b></div>` : ''}
      </div>`, direction === 'LONG' ? 'Comprar' : 'Vender', direction === 'LONG' ? 'btn-buy' : 'btn-sell');
    if (!ok) return;
    const btns = [$('#btn-long'), $('#btn-short')];
    btns.forEach((b) => { b.disabled = true; });
    const res = await run('open', args, (d) => (d.trade ? `${direction} ${S.symbol} abierta @ ${fp(d.trade.entry_price, S.symbol)}` : `Orden límite enviada (${d.qty} @ ${d.price})`));
    btns.forEach((b) => { b.disabled = false; });
    if (res) $('#open-amount').value = '';
    renderEstimates();
  }

  function leverageModal(symbol, current, onApply, max = 125) {
    openModal({
      title: `Ajustar leverage · ${symbol}`,
      body: `
        <div class="row-between"><input type="range" id="lev-range" min="1" max="${max}" value="${current}" style="flex:1;accent-color:var(--yellow)">
          <b id="lev-val" style="min-width:48px;text-align:right">${current}x</b></div>
        <div class="pct-row">${[1, 3, 5, 10, 20, 50, 75, 125].filter((x) => x <= max).map((x) => `<button type="button" data-l="${x}">${x}x</button>`).join('')}</div>
        <p class="muted small">El cambio de leverage es la única operación que Binance solo permite por REST; se envía por los proxies configurados y se cachea para no repetirla.</p>`,
      buttons: [
        { label: 'Cancelar', onClick: closeModal },
        { label: 'Aplicar', cls: 'btn-primary', onClick: async (b) => { b.disabled = true; await onApply(+$('#lev-range').value); closeModal(); } },
      ],
      onOpen: (body) => {
        const r = $('#lev-range', body);
        r.oninput = () => { $('#lev-val').textContent = `${r.value}x`; };
        body.querySelectorAll('[data-l]').forEach((b) => { b.onclick = () => { r.value = b.dataset.l; r.oninput(); }; });
      },
    });
  }

  // ── Grid: formulario y vista previa ────────────────────────────────────
  function setGridRange(pct) {
    const p = S.ticker ? (S.ticker.last || S.ticker.mark) : (S.marketMap.get(S.symbol) || [])[1];
    if (!p) return;
    const d = priceDec(S.symbol, p);
    $('#g-lower').value = (p * (1 - pct / 100)).toFixed(d);
    $('#g-upper').value = (p * (1 + pct / 100)).toFixed(d);
    previewGrid();
  }
  function gridArgs() {
    return {
      symbol: S.symbol, mode: S.gridMode, lower: num($('#g-lower').value), upper: num($('#g-upper').value),
      grids: num($('#g-grids').value), spacing: $('#g-spacing').value, investment: num($('#g-invest').value),
      leverage: num($('#g-lev').value), stop_loss: num($('#g-sl').value), take_profit: num($('#g-tp').value),
      trigger_price: num($('#g-trigger').value), close_on_stop: $('#g-close').checked, post_only: $('#g-postonly').checked,
    };
  }
  const previewGrid = debounce(async () => {
    const box = $('#grid-preview');
    const a = gridArgs();
    if (!a.lower || !a.upper || !a.grids || !a.investment) {
      box.innerHTML = '<p class="muted small">Completa rango, grids e inversión para ver la vista previa.</p>';
      $('#grid-create').disabled = true;
      return;
    }
    let p;
    try { p = await WS.cmd('grid_preview', a); } catch (e) {
      box.innerHTML = `<div class="err-line">${esc(e.message)}</div>`;
      $('#grid-create').disabled = true;
      return;
    }
    const sym = a.symbol;
    box.innerHTML = `
      <div class="kv"><span>Precio actual</span><b>${fp(p.mark, sym)}</b></div>
      <div class="kv"><span>Beneficio por grid</span><b>${fn(p.profit_per_grid_pct[0], 3)}% – ${fn(p.profit_per_grid_pct[1], 3)}%</b></div>
      <div class="kv"><span>Neto de comisiones (maker)</span><b class="${cl(p.net_profit_per_grid_pct[0])}">${fn(p.net_profit_per_grid_pct[0], 3)}% – ${fn(p.net_profit_per_grid_pct[1], 3)}%</b></div>
      <div class="kv"><span>Cantidad por grid</span><b>${esc(p.qty_per_grid)} ${esc(baseOf(sym))}</b></div>
      <div class="kv"><span>Notional total</span><b>${fn(p.total_notional)} USDT</b></div>
      <div class="kv"><span>Margen requerido</span><b>${fn(p.margin_required)} USDT</b></div>
      <div class="kv"><span>Inversión mínima</span><b>${fn(p.min_investment)} USDT</b></div>
      ${p.initial_position_qty ? `<div class="kv"><span>Posición inicial a mercado</span><b>${fq(p.initial_position_qty)} ${esc(baseOf(sym))}</b></div>` : ''}
      ${p.warnings.map((w) => `<div class="warn-line">⚠ ${esc(w)}</div>`).join('')}
      ${p.errors.map((w) => `<div class="err-line">✕ ${esc(w)}</div>`).join('')}`;
    $('#grid-create').disabled = p.errors.length > 0;
  }, 350);

  async function createGrid() {
    const a = gridArgs();
    const modeTxt = { LONG: 'Long', SHORT: 'Short', NEUTRAL: 'Neutral' }[a.mode];
    const ok = await confirmBox('Crear bot Grid', `
      <div class="kv-list">
        <div><span>Símbolo</span><b>${esc(a.symbol)}</b></div>
        <div><span>Modo</span><b>${modeTxt} · ${a.spacing === 'GEOMETRIC' ? 'geométrico' : 'aritmético'}</b></div>
        <div><span>Rango</span><b>${fp(a.lower, a.symbol)} – ${fp(a.upper, a.symbol)}</b></div>
        <div><span>Grids</span><b>${a.grids}</b></div>
        <div><span>Inversión</span><b>${fn(a.investment)} USDT · ${a.leverage}x</b></div>
        ${a.stop_loss ? `<div><span>Stop loss</span><b class="down">${fp(a.stop_loss, a.symbol)}</b></div>` : ''}
        ${a.take_profit ? `<div><span>Take profit</span><b class="up">${fp(a.take_profit, a.symbol)}</b></div>` : ''}
        ${a.trigger_price ? `<div><span>Activación</span><b>${fp(a.trigger_price, a.symbol)}</b></div>` : ''}
      </div>
      <p class="muted small">Se colocarán ${a.grids} órdenes límite por WebSocket. Los fills llegan en tiempo real por el User Data Stream.</p>`, 'Crear bot', 'btn-primary');
    if (!ok) return;
    $('#grid-create').disabled = true;
    const res = await run('grid_create', a, (d) => `Grid ${d.symbol} creado (${d.status === 'PENDING' ? 'esperando activación' : 'iniciando'})`);
    $('#grid-create').disabled = false;
    if (res) switchTab('grids');
  }

  // ── Tablas ─────────────────────────────────────────────────────────────
  function filtered(rows) { return S.onlySym ? rows.filter((r) => r.symbol === S.symbol) : rows; }
  function emptyRow(cols, text) { return `<tr class="empty-row"><td colspan="${cols}">${text}</td></tr>`; }
  const SRC = { signal: 'Señal', manual: 'Manual', grid: 'Grid', externa: 'Externa' };

  function renderPositions() {
    const rows = filtered(S.snap ? S.snap.positions : []);
    const head = `<thead><tr><th>Símbolo</th><th class="r">Tamaño</th><th class="r">Entrada</th><th class="r">Marca</th>
      <th class="r">Liq.</th><th class="r">Margen</th><th class="r">PnL (ROE %)</th><th class="r">TP / SL</th><th>Origen</th><th class="r">Cerrar</th></tr></thead>`;
    const body = rows.length ? rows.map((p) => `
      <tr data-sym="${p.symbol}" data-dir="${p.direction}">
        <td class="sym ${dcls(p.direction)}"><b class="link" data-act="select">${esc(p.symbol)}</b>
          <span class="badge ${dcls(p.direction)}">${p.direction === 'LONG' ? 'Long' : 'Short'}</span> <span class="badge">${p.leverage}x</span>
          ${p.assumed ? '<span class="badge y" title="Registrada sin orden real (margen insuficiente)">ASUMIDA</span>' : ''}
          <span class="sub">${p.margin_type === 'isolated' ? 'Aislado' : p.margin_type ? 'Cruzado' : ''}${p.trade_id ? ` · #${p.trade_id}` : ''}${p.paper_id ? ` · paper #${p.paper_id}` : ''}</span></td>
        <td class="r ${p.direction === 'LONG' ? 'up' : 'down'}">${fq(p.qty)} ${esc(baseOf(p.symbol))}<span class="sub">${fn(p.notional)} USDT</span></td>
        <td class="r">${fp(p.entry, p.symbol)}${p.break_even ? `<span class="sub">BE ${fp(p.break_even, p.symbol)}</span>` : ''}</td>
        <td class="r">${fp(p.mark, p.symbol)}</td>
        <td class="r ${p.liq ? 'text-yellow' : ''}">${p.liq ? fp(p.liq, p.symbol) : '–'}</td>
        <td class="r">${fn(p.margin)} USDT</td>
        <td class="r ${cl(p.pnl)}"><b>${sg(p.pnl, 4)} USDT</b><span class="sub ${cl(p.roe)}">${sg(p.roe)}%</span></td>
        <td class="r"><span class="up">${p.tp ? fp(p.tp, p.symbol) : '–'}</span> / <span class="down">${p.sl ? fp(p.sl, p.symbol) : '–'}</span>
          <button class="btn-link" data-act="tpsl" title="Editar TP/SL">✎</button></td>
        <td><span class="badge${p.source === 'grid' ? ' neutral' : p.source === 'externa' ? ' y' : ''}">${SRC[p.source] || esc(p.source)}</span></td>
        <td><div class="acts">
          <button class="btn btn-xs btn-ghost" data-act="close">Mercado</button>
          <button class="btn btn-xs btn-ghost" data-act="manage" title="Leverage, margen, cierre límite">⋯</button></div></td>
      </tr>`).join('') : emptyRow(10, 'Sin posiciones abiertas');
    $('#tbl-positions').innerHTML = head + `<tbody>${body}</tbody>`;
  }

  function renderOrders() {
    const rows = filtered(S.snap ? S.snap.orders : []);
    const TYPE = { LIMIT: 'Límite', MARKET: 'Mercado', TAKE_PROFIT_MARKET: 'TP mercado', STOP_MARKET: 'SL mercado', TAKE_PROFIT: 'TP límite', STOP: 'SL límite', TRAILING_STOP_MARKET: 'Trailing' };
    const head = `<thead><tr><th>Hora</th><th>Símbolo</th><th>Tipo</th><th>Lado</th><th class="r">Precio</th><th class="r">Disparo</th>
      <th class="r">Cantidad</th><th class="r">Ejecutado</th><th>Reduce</th><th>Origen</th><th class="r"></th></tr></thead>`;
    const body = rows.length ? rows.map((o) => `
      <tr data-sym="${o.symbol}" data-id="${o.id}" data-kind="${o.kind}">
        <td>${dt(o.time)}</td><td><b>${esc(o.symbol)}</b>${o.position_side !== 'BOTH' ? ` <span class="badge">${esc(o.position_side)}</span>` : ''}</td>
        <td>${esc(TYPE[o.type] || o.type)}</td>
        <td class="${o.side === 'BUY' ? 'up' : 'down'}">${o.side === 'BUY' ? 'Compra' : 'Venta'}</td>
        <td class="r">${o.price ? fp(o.price, o.symbol) : '–'}</td><td class="r">${o.trigger_price ? fp(o.trigger_price, o.symbol) : '–'}</td>
        <td class="r">${o.close_position ? 'Cerrar todo' : fq(o.qty)}</td><td class="r">${fq(o.filled)}</td>
        <td>${o.reduce_only || o.close_position ? 'Sí' : 'No'}</td><td><span class="badge">${esc(o.origin)}</span></td>
        <td><div class="acts"><button class="btn btn-xs btn-ghost" data-act="cancel">Cancelar</button></div></td>
      </tr>`).join('') : emptyRow(11, 'Sin órdenes abiertas');
    $('#tbl-orders').innerHTML = head + `<tbody>${body}</tbody>`;
  }

  const GST = { RUNNING: ['En marcha', 'ok'], PENDING: ['Esperando activación', 'y'], STARTING: ['Iniciando', 'y'], STOPPING: ['Deteniendo', 'y'], STOPPED: ['Detenido', ''], ERROR: ['Error', 'err'] };
  function renderGrids() {
    const box = $('#grid-cards');
    const rows = filtered(S.snap ? S.snap.grids : []);
    if (!rows.length) { box.innerHTML = '<div class="empty">No hay bots Grid. Créalo desde el panel derecho → «Bot Grid».</div>'; return; }
    box.innerHTML = rows.map((g) => {
      const [stTxt, stCls] = GST[g.status] || [g.status, ''];
      const pos = g.mark && g.upper > g.lower ? Math.min(100, Math.max(0, ((g.mark - g.lower) / (g.upper - g.lower)) * 100)) : null;
      const modeCls = g.mode === 'LONG' ? 'long' : g.mode === 'SHORT' ? 'short' : 'neutral';
      return `<div class="gcard" data-id="${g.id}">
        <header><b>${esc(g.symbol)}</b><span class="badge ${modeCls}">${{ LONG: 'Long', SHORT: 'Short', NEUTRAL: 'Neutral' }[g.mode]}</span>
          <span class="badge">${g.leverage}x</span><span class="badge st ${stCls}">${stTxt}</span></header>
        <div><span class="muted small">PnL total</span><div class="pnl ${cl(g.total_pnl)}">${sg(g.total_pnl, 4)} USDT <small>(${sg(g.total_pct)}%)</small></div></div>
        <div class="kvs">
          <div><span>Beneficio grid</span><b class="${cl(g.grid_profit)}">${sg(g.grid_profit, 4)}</b></div>
          <div><span>No realizado</span><b class="${cl(g.unrealized)}">${sg(g.unrealized, 4)}</b></div>
          <div><span>Ciclos</span><b>${g.matched}</b></div>
          <div><span>APR</span><b>${g.apr == null ? '–' : `${fn(g.apr, 1)}%`}</b></div>
          <div><span>Inversión</span><b>${fn(g.investment)}</b></div>
          <div><span>Comisiones</span><b>${fn(g.fees, 4)}</b></div>
          <div><span>Rango</span><b>${fp(g.lower, g.symbol)} – ${fp(g.upper, g.symbol)}</b></div>
          <div><span>Grids</span><b>${g.grids} · ${g.spacing === 'GEOMETRIC' ? 'geom.' : 'arit.'}</b></div>
          <div><span>Inventario</span><b>${g.inventory_long ? `L ${fq(g.inventory_long)}` : ''}${g.inventory_short ? ` S ${fq(g.inventory_short)}` : ''}${!g.inventory_long && !g.inventory_short ? '0' : ''}</b></div>
          <div><span>Tiempo</span><b>${dur(g.runtime_s)}</b></div>
        </div>
        ${pos != null && g.status !== 'STOPPED' ? `<div class="bar" title="Precio dentro del rango"><i style="left:${pos}%"></i></div>` : ''}
        ${g.last_error ? `<div class="gerr">${esc(g.last_error)}</div>` : ''}
        ${g.stop_reason && g.status === 'STOPPED' ? `<div class="muted small">Detenido: ${esc(g.stop_reason)}</div>` : ''}
        <footer>
          <button class="btn btn-xs btn-ghost" data-gact="detail">Detalles</button>
          ${g.status === 'STOPPED' || g.status === 'ERROR' ? '<button class="btn btn-xs btn-ghost" data-gact="delete">Eliminar</button>' : '<button class="btn btn-xs btn-danger" data-gact="stop">Detener</button>'}
        </footer></div>`;
    }).join('');
  }

  const REASON = { TP: ['Take profit', 'ok'], SL: ['Stop loss', 'err'], MANUAL: ['Manual', ''], MAIN_BOT: ['Señal', ''], CLOSE_ALL: ['Cierre global', ''], EXTERNAL: ['Externo', 'y'], LIQUIDATION: ['Liquidación', 'err'], ADL: ['ADL', 'err'], NETTED: ['Neteada', ''] };
  function renderHistory() {
    const rows = filtered(S.snap ? S.snap.closed : []);
    const st = S.snap.stats;
    $('#hist-summary').textContent = `PnL realizado ${sg(st.realized_pnl, 4)} USDT · ${st.wins} ganadas / ${st.losses} perdidas · comisiones ${fn(st.fees, 4)} USDT`;
    const head = `<thead><tr><th>Cierre</th><th>Símbolo</th><th class="r">Cantidad</th><th class="r">Entrada</th><th class="r">Salida</th>
      <th class="r">PnL</th><th class="r">ROE</th><th>Motivo</th><th>Origen</th><th class="r">Duración</th></tr></thead>`;
    const body = rows.length ? rows.map((t) => {
      const [rTxt, rCls] = REASON[t.status] || [t.status, ''];
      return `<tr><td>${esc(t.close_time.replace(' UTC', ''))}</td>
        <td class="sym ${dcls(t.direction)}"><b>${esc(t.symbol)}</b> <span class="badge ${dcls(t.direction)}">${t.direction === 'LONG' ? 'Long' : 'Short'}</span> <span class="badge">${t.leverage}x</span></td>
        <td class="r">${fq(t.quantity)}</td><td class="r">${fp(t.entry_price, t.symbol)}</td><td class="r">${fp(t.close_price, t.symbol)}</td>
        <td class="r ${cl(t.pnl_usdt)}"><b>${sg(t.pnl_usdt, 4)}</b></td><td class="r ${cl(t.roe_pct)}">${sg(t.roe_pct)}%</td>
        <td><span class="badge ${rCls}">${esc(rTxt)}</span></td><td><span class="badge">${SRC[t.source] || esc(t.source)}</span></td>
        <td class="r">${t.closed_ts && t.opened_ts ? dur(t.closed_ts - t.opened_ts) : '–'}</td></tr>`;
    }).join('') : emptyRow(10, 'Sin operaciones cerradas');
    $('#tbl-history').innerHTML = head + `<tbody>${body}</tbody>`;
  }

  function renderSignals() {
    const s = S.snap.status;
    const cards = [
      ['Recibidas', s.signals_received], ['Aperturas', s.signals_open], ['Cierres', s.signals_close], ['Rechazadas', s.signals_rejected],
      ['TP / SL creados', `${s.signals_tp_set} / ${s.signals_sl_set}`], ['Cierres manuales', s.manual_closes],
      ['Última señal', `${s.last_signal_time}`],
    ];
    $('#signal-cards').innerHTML = cards.map(([k, v]) => `<div class="card"><span>${k}</span><b>${esc(v)}</b></div>`).join('');
    const rows = S.snap.signals.filter((r) => !S.onlySym || r.symbol === S.symbol);
    const head = '<thead><tr><th>Hora</th><th>Acción</th><th>Símbolo</th><th>Dirección</th><th>Resultado</th><th>Detalle</th></tr></thead>';
    const body = rows.length ? rows.map((r) => `<tr><td>${hms(r.ts)}</td><td><b>${esc(r.action.toUpperCase())}</b></td><td>${esc(r.symbol)}</td>
      <td>${r.direction ? `<span class="badge ${dcls(r.direction)}">${esc(r.direction)}</span>` : ''}</td>
      <td><span class="badge ${r.ok ? 'ok' : 'err'}">${r.ok ? 'OK' : 'Rechazada'}</span></td><td class="muted">${esc(r.detail)}</td></tr>`).join('')
      : emptyRow(6, 'Aún no llegan señales de app.py');
    $('#tbl-signals').innerHTML = head + `<tbody>${body}</tbody>`;
  }

  function renderErrors() {
    const list = $('#err-list');
    if (!list) return;
    const errs = S.errors;
    const fixed = errs.filter((e) => e.fixed).length;
    const crit = errs.filter((e) => e.severity === 'critical').length;
    $('#err-summary').textContent = errs.length ? `${errs.length} errores · ${fixed} autocorregidos · ${crit} críticos` : '';
    $('#n-errors').textContent = errs.length;
    if (!errs.length) { list.innerHTML = '<div class="empty">Sin errores registrados 🎉</div>'; return; }
    list.innerHTML = errs.slice(0, 150).map((e) => `
      <div class="err-item ${e.severity}">
        <div class="top"><span class="badge err code-badge" data-code="${e.code || e.http_status}">${e.code || (e.http_status ? `HTTP ${e.http_status}` : 'red')}</span>
          <b>${esc(e.title)}</b>${e.symbol ? `<span class="badge">${esc(e.symbol)}</span>` : ''}
          ${e.fixed ? '<span class="badge ok">✓ autocorregido</span>' : ''}<span class="when">${esc(e.where)} · ${hms(e.ts)}</span></div>
        <div class="msg">${esc(e.msg)}</div>
        <div class="sol">💡 ${esc(e.solution)}${e.fix_note ? ` <span class="muted">— ${esc(e.fix_note)}</span>` : ''}</div>
      </div>`).join('');
  }

  function explainResult(d) {
    return `<h5><span class="badge err code-badge">${esc(d.code)}</span> ${esc(d.title)}</h5>
      <div class="muted small">${esc(d.name)} · categoría ${esc(d.category)} · severidad ${esc(d.severity)}</div>
      <div><div class="lbl">Causa</div>${esc(d.cause)}</div>
      <div><div class="lbl">Solución</div>${esc(d.solution)}</div>
      <div><div class="lbl">Acción automática del executor</div>${esc(d.action_label)}${d.retryable ? ' <span class="badge ok">reintenta</span>' : ''}</div>
      ${d.known ? '' : '<div class="text-yellow small">Código fuera del catálogo: se muestra la guía genérica.</div>'}`;
  }

  async function explain(code) {
    $('#err-code').value = code;
    const d = await run('explain_error', { code });
    if (d) $('#err-result').innerHTML = explainResult(d);
  }

  async function showCatalog() {
    if (!S.catalog) S.catalog = await run('error_catalog', {});
    if (!S.catalog) return;
    const rowsHtml = (q) => S.catalog.filter((c) => !q || `${c.code} ${c.name} ${c.title} ${c.category}`.toLowerCase().includes(q))
      .map((c) => `<tr data-code="${c.code}" style="cursor:pointer"><td><b class="code-badge">${c.code}</b></td><td>${esc(c.title)}<span class="sub">${esc(c.name)}</span></td><td class="muted">${esc(c.action_label)}</td></tr>`).join('');
    openModal({
      title: `Catálogo de errores de Binance (${S.catalog.length})`, wide: true,
      body: `<input id="cat-q" class="inline-form" placeholder="Filtrar (código, nombre, palabra)" style="height:32px;border-radius:6px;border:1px solid var(--line);background:var(--panel-2);padding:0 10px;outline:0">
        <div class="table-wrap" style="max-height:60vh"><table class="tbl"><thead><tr><th>Código</th><th>Significado</th><th>Corrección automática</th></tr></thead><tbody id="cat-body">${rowsHtml('')}</tbody></table></div>`,
      onOpen: (body) => {
        $('#cat-q', body).oninput = (e) => { $('#cat-body').innerHTML = rowsHtml(e.target.value.toLowerCase()); };
        $('#cat-body', body).parentElement.onclick = (e) => {
          const tr = e.target.closest('tr[data-code]');
          if (tr) { closeModal(); switchTab('errors'); explain(tr.dataset.code); }
        };
      },
    });
  }

  function sysCard(title, ok, rows) {
    return `<div class="sys-card"><h5><i class="dot ${ok === null ? '' : ok ? 'on' : 'off'}"></i>${title}</h5>${rows.map(([k, v]) => `<div><span>${k}</span><b title="${esc(v)}">${esc(v)}</b></div>`).join('')}</div>`;
  }
  function renderSystem() {
    const h = S.snap.health, cfg = S.snap.settings;
    const rl = Object.entries(h.ws_api.rate_limits || {}).map(([k, v]) => [k, `${v.count ?? '?'} / ${v.limit ?? '?'}`]);
    const restRows = Object.entries(h.rest.calls || {}).map(([k, v]) => [k, v]);
    const ex = h.exchange_info;
    $('#sys-grid').innerHTML = [
      sysCard('WebSocket API', h.ws_api.connected, [['Latencia', `${fn(h.ws_api.latency_ms, 1)} ms`], ['Peticiones', h.ws_api.requests], ['Errores', h.ws_api.errors], ['Reconexiones', h.ws_api.reconnects], ['Activa', dur(h.ws_api.uptime_s)], ...rl]),
      sysCard('Stream de mercado', h.market.connected, [['Símbolos con mark', h.market.symbols], ['Tickers 24h', h.market.tickers], ['Mensajes', h.market.messages], ['Último mensaje', h.market.last_message_age_s == null ? '–' : `hace ${h.market.last_message_age_s}s`], ['Reconexiones', h.market.reconnects]]),
      sysCard('User Data Stream', h.user.disabled ? null : h.user.connected, h.user.disabled ? [['Estado', 'sin credenciales']] : [['Eventos', h.user.events], ['Reconexiones', h.user.reconnects], ['Activo', dur(h.user.uptime_s)], ['Último error', h.user.last_error || '–']]),
      sysCard('REST (solo lo imprescindible)', null, [['Llamadas totales', h.rest.total], ...restRows, ['Proxies', (h.rest.proxies || []).join(', ') || 'ninguno'], ['Última ruta', h.rest.last_route || '–']]),
      sysCard('exchangeInfo local', ex.count > 0, [['Símbolos', ex.count], ['Generado', ex.generated_at || '—'], ['Antigüedad', ex.age_h == null ? '—' : `${ex.age_h} h`], ['Reglas aprendidas', ex.learned], ['Eventos contractInfo', ex.contract_updates], ['Origen', ex.source || '—']]),
      sysCard('Reloj y notificaciones', null, [['Desfase con Binance', `${sg(h.clock_offset_ms, 0)} ms`], ['Telegram', h.telegram.enabled ? `activo (${h.telegram.sent} enviados)` : 'desactivado']]),
      sysCard('Configuración', null, [['Entorno', cfg.env], ['Leverage señales', `${cfg.leverage}x (>${cfg.high_price_threshold} USDT → ${cfg.high_price_leverage}x)`], ['Notional mín.', `${cfg.min_notional_usdt} USDT +${cfg.notional_buffer_pct}%`], ['Tamaño por defecto', cfg.default_notional_usdt ? `${cfg.default_notional_usdt} USDT` : 'desactivado'], ['Bloqueados', (cfg.blocked_symbols || []).join(', ') || 'ninguno'], ['Margen insuf. → asumida', cfg.assume_on_margin_error ? 'sí' : 'no']]),
    ].join('');
    renderLogs();
  }
  function renderLogs() {
    const box = $('#logbox');
    const atBottom = box.scrollTop + box.clientHeight >= box.scrollHeight - 20;
    box.innerHTML = S.logs.slice(-300).map((l) => `<span class="${l.level}">${hms(l.ts)} ${l.level.padEnd(7)} ${esc(l.name)}: ${esc(l.msg)}</span>`).join('\n');
    if (atBottom) box.scrollTop = box.scrollHeight;
  }

  // ── Modales de gestión ─────────────────────────────────────────────────
  function findPos(sym, dir) { return (S.snap ? S.snap.positions : []).find((p) => p.symbol === sym && p.direction === dir); }

  function tpslModal(p) {
    const sym = p.symbol, lev = p.leverage || 1, entry = p.entry, qty = p.qty, sgn = p.direction === 'LONG' ? 1 : -1;
    const d = priceDec(sym, entry);
    const pnlAt = (price) => (price - entry) * qty * sgn;
    openModal({
      title: `TP / SL · ${sym} ${p.direction}`,
      body: `
        <div class="kv-list"><div><span>Entrada</span><b>${fp(entry, sym)}</b></div><div><span>Marca</span><b>${fp(p.mark, sym)}</b></div>
          <div><span>Cantidad</span><b>${fq(qty)}</b></div><div><span>Leverage</span><b>${lev}x</b></div></div>
        <label class="field"><span>Take profit</span><input id="m-tp" type="number" step="any" value="${p.tp || ''}"><em id="m-tp-pnl"></em></label>
        <div class="pct-row compact"><span class="muted small">ROE:</span>${[10, 25, 50, 100, 200].map((x) => `<button type="button" data-tp="${x}">+${x}%</button>`).join('')}</div>
        <label class="field"><span>Stop loss</span><input id="m-sl" type="number" step="any" value="${p.sl || ''}"><em id="m-sl-pnl"></em></label>
        <div class="pct-row compact"><span class="muted small">ROE:</span>${[10, 25, 50, 75, 90].map((x) => `<button type="button" data-sl="${x}">-${x}%</button>`).join('')}</div>
        <p class="muted small">Se crean como algo orders (STOP_MARKET / TAKE_PROFIT_MARKET) por WebSocket, con disparo por mark price.</p>`,
      buttons: [
        { label: 'Cancelar TP/SL', cls: 'btn-ghost', onClick: async () => { await run('cancel_tp_sl', { symbol: sym, direction: p.direction }, (r) => `${r.cancelled} orden(es) canceladas`); closeModal(); } },
        { label: 'Guardar', cls: 'btn-primary', onClick: async (b) => {
          b.disabled = true;
          const tp = num($('#m-tp').value), sl = num($('#m-sl').value);
          if (tp && tp !== p.tp) await run('set_tp', { symbol: sym, direction: p.direction, trigger_price: tp }, `TP ${sym} @ ${fp(tp, sym)}`);
          if (sl && sl !== p.sl) await run('set_sl', { symbol: sym, direction: p.direction, trigger_price: sl }, `SL ${sym} @ ${fp(sl, sym)}`);
          closeModal();
        } },
      ],
      onOpen: (body) => {
        const upd = () => {
          const tp = num($('#m-tp').value), sl = num($('#m-sl').value);
          $('#m-tp-pnl').innerHTML = tp ? `<span class="${cl(pnlAt(tp))}">${sg(pnlAt(tp))}</span>` : 'USDT';
          $('#m-sl-pnl').innerHTML = sl ? `<span class="${cl(pnlAt(sl))}">${sg(pnlAt(sl))}</span>` : 'USDT';
        };
        body.addEventListener('input', upd);
        body.querySelectorAll('[data-tp]').forEach((b) => { b.onclick = () => { $('#m-tp').value = (entry * (1 + (sgn * b.dataset.tp) / 100 / lev)).toFixed(d); upd(); }; });
        body.querySelectorAll('[data-sl]').forEach((b) => { b.onclick = () => { $('#m-sl').value = (entry * (1 - (sgn * b.dataset.sl) / 100 / lev)).toFixed(d); upd(); }; });
        upd();
      },
    });
  }

  function manageModal(p) {
    const sym = p.symbol;
    openModal({
      title: `Gestionar ${sym} ${p.direction}`,
      body: `
        <div class="kv-list"><div><span>Tamaño</span><b>${fq(p.qty)}</b></div><div><span>Margen</span><b>${fn(p.margin)} USDT (${p.margin_type || '—'})</b></div></div>
        <h4 style="margin:6px 0 0">Cierre límite (reduce only)</h4>
        <div class="row-2"><label class="field"><span>Precio</span><input id="mg-price" type="number" step="any" value="${(p.mark || p.entry).toFixed(priceDec(sym, p.mark))}"></label>
          <label class="field"><span>Cantidad</span><input id="mg-qty" type="number" step="any" value="${p.qty}"></label></div>
        <button type="button" class="btn btn-ghost btn-sm" id="mg-limit">Enviar cierre límite</button>
        <h4 style="margin:6px 0 0">Margen aislado</h4>
        <div class="inline-form"><input id="mg-amt" type="number" step="any" placeholder="USDT">
          <button type="button" class="btn btn-ghost btn-sm" data-margin="1">Añadir</button><button type="button" class="btn btn-ghost btn-sm" data-margin="0">Retirar</button></div>
        <h4 style="margin:6px 0 0">Tipo de margen y leverage</h4>
        <div class="inline-form"><button type="button" class="btn btn-ghost btn-sm" data-mt="ISOLATED">Aislado</button><button type="button" class="btn btn-ghost btn-sm" data-mt="CROSSED">Cruzado</button>
          <button type="button" class="btn btn-ghost btn-sm" id="mg-lev">Leverage ${p.leverage}x</button></div>`,
      buttons: [{ label: 'Cerrar', onClick: closeModal }],
      onOpen: (body) => {
        $('#mg-limit', body).onclick = () => run('limit_order', {
          symbol: sym, side: p.direction === 'LONG' ? 'SELL' : 'BUY', price: num($('#mg-price').value), quantity: num($('#mg-qty').value),
          reduce_only: true, direction: p.direction,
        }, 'Orden de cierre límite enviada');
        body.querySelectorAll('[data-margin]').forEach((b) => { b.onclick = () => run('modify_margin', { symbol: sym, direction: p.direction, amount: num($('#mg-amt').value), add: b.dataset.margin === '1' }, 'Margen actualizado'); });
        body.querySelectorAll('[data-mt]').forEach((b) => { b.onclick = () => run('set_margin_type', { symbol: sym, margin_type: b.dataset.mt }, `Margen ${b.dataset.mt === 'ISOLATED' ? 'aislado' : 'cruzado'}`); });
        $('#mg-lev', body).onclick = () => leverageModal(sym, p.leverage, (lev) => run('set_symbol_leverage', { symbol: sym, leverage: lev }, (r) => `Leverage aplicado: ${r.applied}x`));
      },
    });
  }

  async function gridDetailModal(id) {
    const d = await run('grid_detail', { id });
    if (!d) return;
    S.gridDetail = id;
    openModal({ title: `Grid ${d.symbol} · ${d.mode}`, wide: true, body: gridDetailHtml(d), buttons: [{ label: 'Cerrar', onClick: closeModal }] });
  }
  async function refreshGridDetail() {
    if (refreshGridDetail.busy) return;
    refreshGridDetail.busy = true;
    try {
      const d = await WS.cmd('grid_detail', { id: S.gridDetail });
      if (S.gridDetail === d.id) $('#modal-body').innerHTML = gridDetailHtml(d);
    } catch { /* el modal pudo cerrarse */ } finally { refreshGridDetail.busy = false; }
  }
  function gridDetailHtml(d) {
    const cells = d.cells.slice().reverse();
    const STATE = { WAIT_OPEN: 'Esperando entrada', HOLD: 'Con posición' };
    return `<div class="kv-list">
        <div><span>Estado</span><b>${esc((GST[d.status] || [d.status])[0])}</b></div>
        <div><span>Precio actual</span><b>${fp(d.mark, d.symbol)}</b></div>
        <div><span>Cantidad por grid</span><b>${esc(d.qty_per_grid)}</b></div>
        <div><span>Beneficio grid / ciclos</span><b class="${cl(d.grid_profit)}">${sg(d.grid_profit, 4)} USDT · ${d.matched}</b></div>
        <div><span>PnL total</span><b class="${cl(d.total_pnl)}">${sg(d.total_pnl, 4)} USDT (${sg(d.total_pct)}%)</b></div></div>
      <div class="ladder"><div class="muted small"><span>Celda</span><span>Estado</span><span>Orden</span><span class="r">Ganancia</span></div>
      ${cells.map((c) => {
        const cur = d.mark >= +c.low && d.mark <= +c.high;
        const [side, price] = c.kind === 'LONG' ? (c.state === 'WAIT_OPEN' ? ['BUY', c.low] : ['SELL', c.high]) : (c.state === 'WAIT_OPEN' ? ['SELL', c.high] : ['BUY', c.low]);
        return `<div class="${cur ? 'cur' : ''}"><span>${fp(c.low, d.symbol)} – ${fp(c.high, d.symbol)} <span class="badge ${c.kind.toLowerCase()}">${c.kind === 'LONG' ? 'L' : 'S'}</span></span>
          <span class="small">${STATE[c.state] || c.state}</span>
          <span class="small ${side === 'BUY' ? 'up' : 'down'}">${c.order_id ? `${side === 'BUY' ? 'Compra' : 'Venta'} ${fp(price, d.symbol)}` : (c.last_error ? '<span class="text-red">error</span>' : '—')}</span>
          <span class="r small ${cl(c.profit)}">${c.trades ? `${sg(c.profit, 4)} (${c.trades})` : '–'}</span></div>`;
      }).join('')}</div>`;
  }

  async function stopGridModal(id) {
    const g = S.snap.grids.find((x) => x.id === id);
    if (!g) return;
    openModal({
      title: `Detener grid ${g.symbol}`,
      body: `<p>Se cancelarán todas las órdenes del bot.</p>
        <label class="check"><input type="checkbox" id="stop-close" checked> Cerrar también la posición del grid a mercado</label>
        <div class="kv-list"><div><span>Inventario</span><b>${g.inventory_long ? `Long ${fq(g.inventory_long)}` : ''} ${g.inventory_short ? `Short ${fq(g.inventory_short)}` : ''}${!g.inventory_long && !g.inventory_short ? '0' : ''}</b></div>
          <div><span>PnL total</span><b class="${cl(g.total_pnl)}">${sg(g.total_pnl, 4)} USDT</b></div></div>`,
      buttons: [
        { label: 'Cancelar', onClick: closeModal },
        { label: 'Detener bot', cls: 'btn-sell', onClick: async (b) => { b.disabled = true; await run('grid_stop', { id, close_position: $('#stop-close').checked }, `Grid ${g.symbol} detenido`); closeModal(); } },
      ],
    });
  }

  // ── Pestañas ───────────────────────────────────────────────────────────
  function switchTab(tab) {
    S.tab = tab;
    store.set('tab', tab);
    $$('#tabs button').forEach((b) => b.classList.toggle('active', b.dataset.tab === tab));
    $$('.tab-body').forEach((b) => { b.hidden = b.dataset.body !== tab; });
    renderAll();
    if (tab === 'errors') renderErrors();
  }

  // ── Eventos de la interfaz ─────────────────────────────────────────────
  function bind() {
    $('#login-form').addEventListener('submit', (e) => {
      e.preventDefault();
      const token = $('#login-token').value.trim();
      if (!token) return;
      WS.token = token;
      if ($('#login-remember').checked) store.set('token', token);
      $('#login-error').textContent = '';
      WS.denied = false;
      if (WS.sock && WS.sock.readyState === 1) WS.send({ op: 'auth', token }); else WS.connect();
    });
    $('#logout-btn').onclick = () => { store.del('token'); WS.token = ''; location.reload(); };
    $('#theme-btn').onclick = () => {
      const next = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark';
      document.documentElement.dataset.theme = next;
      store.set('theme', next);
      Chart.requestDraw();
    };
    $('#trading-toggle').onchange = async (e) => {
      const enabled = e.target.checked;
      if (!enabled && !(await confirmBox('Pausar trading', '<p>Las nuevas señales de apertura se rechazarán. Las posiciones abiertas, cierres, TP/SL y grids siguen funcionando.</p>', 'Pausar', 'btn-sell'))) { e.target.checked = true; return; }
      await run('toggle_trading', { enabled }, enabled ? 'Trading activado' : 'Trading pausado');
    };
    $('#lev-btn').onclick = () => leverageModal('señales (por defecto)', S.snap ? S.snap.leverage : 4, (lev) => run('set_default_leverage', { leverage: lev }, `Leverage por defecto: ${lev}x`));

    // Mercados
    $('#mkt-search').addEventListener('input', debounce((e) => { S.search = e.target.value; renderMarkets(); }, 120));
    $$('#mkt-sort button').forEach((b) => {
      b.classList.toggle('active', b.dataset.sort === S.sort);
      b.onclick = () => { S.sort = b.dataset.sort; store.set('sort', S.sort); $$('#mkt-sort button').forEach((x) => x.classList.toggle('active', x === b)); renderMarkets(); };
    });
    $('#mkt-list').addEventListener('click', (e) => {
      const star = e.target.closest('[data-star]');
      if (star) { e.stopPropagation(); toggleFav(star.dataset.star); return; }
      const row = e.target.closest('.mkt-row');
      if (row) selectSymbol(row.dataset.sym);
    });
    $('#sym-fav').onclick = () => toggleFav(S.symbol);

    // Panel de trading
    $$('#trade-tabs button').forEach((b) => {
      b.onclick = () => {
        S.tradeTab = b.dataset.tab;
        $$('#trade-tabs button').forEach((x) => x.classList.toggle('active', x === b));
        $('#open-form').hidden = S.tradeTab !== 'open';
        $('#grid-form').hidden = S.tradeTab !== 'grid';
        if (S.tradeTab === 'grid') previewGrid();
      };
    });
    $$('#ord-type button').forEach((b) => {
      b.onclick = () => {
        S.ordType = b.dataset.type;
        $$('#ord-type button').forEach((x) => x.classList.toggle('active', x === b));
        $('#limit-field').hidden = S.ordType !== 'LIMIT';
        if (S.ordType === 'LIMIT' && !$('#open-price').value && S.ticker) $('#open-price').value = (S.ticker.last || S.ticker.mark).toFixed(priceDec(S.symbol, S.ticker.last));
        renderEstimates();
      };
    });
    $('#open-lev').onclick = () => {
      const max = (S.ticker && S.ticker.rules && S.ticker.rules.max_leverage) || 125;
      leverageModal(S.symbol, S.openLev || 5, async (lev) => {
        const r = await run('set_symbol_leverage', { symbol: S.symbol, leverage: lev }, (x) => `Leverage ${S.symbol}: ${x.applied}x`);
        if (r) { S.openLev = r.applied; $('#open-lev').textContent = `${r.applied}x`; renderEstimates(); }
      }, max);
    };
    ['#open-amount', '#open-mode', '#open-price'].forEach((s) => $(s).addEventListener('input', renderEstimates));
    $('#pct-row').addEventListener('click', (e) => {
      const b = e.target.closest('[data-pct]');
      if (!b || !S.snap) return;
      const avail = S.snap.account.available * (+b.dataset.pct / 100), lev = S.openLev || 1, mode = $('#open-mode').value;
      const price = refPrice();
      const v = mode === 'margin' ? avail : mode === 'qty' ? (price ? (avail * lev) / price : 0) : avail * lev;
      $('#open-amount').value = mode === 'qty' ? +v.toPrecision(6) : v.toFixed(2);
      renderEstimates();
    });
    $('#open-tpsl').onchange = (e) => { $('#tpsl-fields').hidden = !e.target.checked; };
    $('#roe-row').addEventListener('click', (e) => { const b = e.target.closest('[data-roe]'); if (b) applyRoe(+b.dataset.roe); });
    ['#open-tp', '#open-sl'].forEach((s) => $(s).addEventListener('input', () => { S.tpslAuto = null; }));
    $('#btn-long').onclick = () => submitOpen('LONG');
    $('#btn-short').onclick = () => submitOpen('SHORT');

    // Grid
    $$('#grid-mode button').forEach((b) => {
      b.onclick = () => { S.gridMode = b.dataset.mode; $$('#grid-mode button').forEach((x) => x.classList.toggle('active', x === b)); previewGrid(); };
    });
    $('#g-range').addEventListener('click', (e) => { const b = e.target.closest('[data-range]'); if (b) setGridRange(+b.dataset.range); });
    $('#grid-form').addEventListener('input', previewGrid);
    $('#grid-create').onclick = createGrid;

    // Pestañas inferiores
    $$('#tabs button').forEach((b) => { b.onclick = () => switchTab(b.dataset.tab); });
    $('#only-sym').onchange = (e) => { S.onlySym = e.target.checked; renderAll(); };
    $('#close-all-btn').onclick = async () => {
      if (!(await confirmBox('Cerrar todas las posiciones', '<p>Se cerrarán a mercado todas las posiciones (excepto las de bots Grid, que se detienen desde su tarjeta).</p>', 'Cerrar todas', 'btn-sell'))) return;
      run('close_all', {}, (r) => `${r.closed} posición(es) cerradas`);
    };
    $('#tbl-positions').addEventListener('click', async (e) => {
      const btn = e.target.closest('[data-act]');
      if (!btn) return;
      const tr = btn.closest('tr');
      const p = findPos(tr.dataset.sym, tr.dataset.dir);
      if (!p) return;
      if (btn.dataset.act === 'select') selectSymbol(p.symbol);
      if (btn.dataset.act === 'tpsl') tpslModal(p);
      if (btn.dataset.act === 'manage') manageModal(p);
      if (btn.dataset.act === 'close') {
        if (!(await confirmBox(`Cerrar ${esc(p.symbol)} ${esc(p.direction)}`, `<p>Cierre a mercado de ${fq(p.qty)} ${esc(baseOf(p.symbol))}. PnL actual <b class="${cl(p.pnl)}">${sg(p.pnl, 4)} USDT</b>.</p>`, 'Cerrar posición', 'btn-sell'))) return;
        run('close_position', { symbol: p.symbol, direction: p.direction }, `${p.symbol} ${p.direction} cerrada`);
      }
    });
    $('#tbl-orders').addEventListener('click', (e) => {
      const btn = e.target.closest('[data-act="cancel"]');
      if (!btn) return;
      const tr = btn.closest('tr');
      run('cancel_order', { symbol: tr.dataset.sym, id: tr.dataset.id, kind: tr.dataset.kind }, 'Orden cancelada');
    });
    $('#grid-cards').addEventListener('click', async (e) => {
      const btn = e.target.closest('[data-gact]');
      if (!btn) return;
      const id = btn.closest('.gcard').dataset.id;
      if (btn.dataset.gact === 'detail') gridDetailModal(id);
      if (btn.dataset.gact === 'stop') stopGridModal(id);
      if (btn.dataset.gact === 'delete' && await confirmBox('Eliminar grid', '<p>Se borrará del historial de bots.</p>', 'Eliminar', 'btn-sell')) run('grid_delete', { id }, 'Grid eliminado');
    });
    $('#clear-history-btn').onclick = async () => {
      if (await confirmBox('Borrar historial', '<p>Se borrará el historial de operaciones cerradas y se reiniciarán los contadores.</p>', 'Borrar', 'btn-sell')) run('clear_history', {}, (r) => `${r.cleared} operaciones borradas`);
    };
    $('#err-lookup').addEventListener('submit', (e) => { e.preventDefault(); explain($('#err-code').value.trim()); });
    $('#err-list').addEventListener('click', (e) => { const b = e.target.closest('[data-code]'); if (b) explain(b.dataset.code); });
    $('#err-catalog-btn').onclick = showCatalog;
    $('#clear-errors-btn').onclick = async () => { if (await run('clear_errors', {})) { S.errors = []; renderErrors(); } };
    $('#sync-btn').onclick = () => run('sync_account', {}, (r) => `Cuenta resincronizada (${r.positions} posiciones)`);
    $('#sync-orders-btn').onclick = () => run('sync_orders', {}, (r) => `${r.orders} órdenes y ${r.algos} TP/SL leídas`);
    $('#exinfo-btn').onclick = () => run('refresh_exchange_info', {}, (r) => `exchangeInfo actualizado: ${r.count} símbolos`);

    $('#modal-x').onclick = closeModal;
    $('#modal').addEventListener('click', (e) => { if (e.target.id === 'modal') closeModal(); });
    document.addEventListener('keydown', (e) => { if (e.key === 'Escape') closeModal(); });
    setInterval(() => { if (S.ticker) renderSymbol(); }, 1000);
  }

  // ── Arranque ───────────────────────────────────────────────────────────
  document.documentElement.dataset.theme = store.get('theme', 'dark');
  bind();
  Chart.init();
  switchTab(S.tab);
  // Acceso directo con el link: se conecta al instante. Solo si el servidor
  // tiene DASHBOARD_TOKEN aparece el formulario de token.
  WS.token = store.get('token', '');
  WS.connect();
})();
