// Nexus KV & Raft Cluster Client Engine
(function() {
  'use strict';

  // State
  let targetBaseUrl = window.location.origin;
  if (!targetBaseUrl || targetBaseUrl === 'null') {
    targetBaseUrl = 'http://localhost:8001';
  }
  
  const clusterPorts = ['8001', '8002', '8003'];
  let currentKeysMap = {};
  let pollInterval = null;

  // DOM Elements
  const nodePills = document.querySelectorAll('.node-pill');
  const customInput = document.getElementById('customTargetInput');
  const healthIndicator = document.getElementById('clusterHealthIndicator');
  const healthText = document.getElementById('clusterHealthText');
  const refreshBtn = document.getElementById('refreshBtn');
  const terminalLog = document.getElementById('terminalLog');
  const clearConsoleBtn = document.getElementById('clearConsoleBtn');

  // Stats
  const statKeys = document.getElementById('statKeys');
  const statWalIdx = document.getElementById('statWalIdx');
  const statSets = document.getElementById('statSets');
  const statGets = document.getElementById('statGets');
  const keyCountBadge = document.getElementById('keyCountBadge');

  // Forms
  const setForm = document.getElementById('setForm');
  const setKeyInput = document.getElementById('setKey');
  const setValInput = document.getElementById('setVal');
  const testFollowerWriteBtn = document.getElementById('testFollowerWriteBtn');

  const getKeyInput = document.getElementById('getKeyInput');
  const getBtn = document.getElementById('getBtn');
  const getResultBox = document.getElementById('getResultBox');
  const getStatusCode = document.getElementById('getStatusCode');
  const getTimeTaken = document.getElementById('getTimeTaken');
  const getResultContent = document.getElementById('getResultContent');

  const triggerSnapshotBtn = document.getElementById('triggerSnapshotBtn');
  const snapshotIntervalInput = document.getElementById('snapshotIntervalInput');
  const saveIntervalBtn = document.getElementById('saveIntervalBtn');

  const searchKeyInput = document.getElementById('searchKeyInput');
  const dataTableBody = document.getElementById('dataTableBody');

  // Helper: Append log line to terminal
  function logEvent(type, message) {
    const line = document.createElement('div');
    line.className = `log-line ${type}`;
    const now = new Date().toLocaleTimeString();
    line.innerHTML = `<span class="timestamp">[${now}]</span> ${escapeHtml(message)}`;
    terminalLog.appendChild(line);
    terminalLog.scrollTop = terminalLog.scrollHeight;
  }

  function escapeHtml(str) {
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  // Determine current active port from targetBaseUrl
  function detectActivePort() {
    try {
      const u = new URL(targetBaseUrl);
      return u.port || (u.protocol === 'https:' ? '443' : '80');
    } catch {
      return '';
    }
  }

  // Update target node selection
  function setTargetNode(url) {
    targetBaseUrl = url.replace(/\/+$/, '');
    const activePort = detectActivePort();
    
    nodePills.forEach(pill => {
      if (pill.dataset.node === activePort) {
        pill.classList.add('active');
      } else if (pill.dataset.node === 'custom' && !clusterPorts.includes(activePort)) {
        pill.classList.add('active');
      } else {
        pill.classList.remove('active');
      }
    });

    clusterPorts.forEach(port => {
      const card = document.getElementById(`cardNode${port}`);
      if (card) {
        if (port === activePort) {
          card.classList.add('active-target');
        } else {
          card.classList.remove('active-target');
        }
      }
    });

    logEvent('info', `Target node changed to: ${targetBaseUrl}`);
    refreshAll();
  }

  // Initialize node picker
  nodePills.forEach(pill => {
    pill.addEventListener('click', () => {
      const nodeType = pill.dataset.node;
      if (nodeType === 'custom') {
        customInput.classList.remove('hidden');
        customInput.focus();
      } else {
        customInput.classList.add('hidden');
        setTargetNode(`http://localhost:${nodeType}`);
      }
    });
  });

  customInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') {
      let val = customInput.value.trim();
      if (!val.startsWith('http://') && !val.startsWith('https://')) {
        val = 'http://' + val;
      }
      setTargetNode(val);
    }
  });

  clearConsoleBtn.addEventListener('click', () => {
    terminalLog.innerHTML = '';
  });

  // Fetch with timeout
  async function fetchWithTimeout(url, options = {}, timeoutMs = 2500) {
    const controller = new AbortController();
    const id = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const response = await fetch(url, {
        ...options,
        signal: controller.signal,
      });
      clearTimeout(id);
      return response;
    } catch (err) {
      clearTimeout(id);
      throw err;
    }
  }

  // Poll cluster nodes status
  async function pollClusterNodes() {
    for (const port of clusterPorts) {
      const start = performance.now();
      const nodeUrl = `http://localhost:${port}`;
      const card = document.getElementById(`cardNode${port}`);
      const badge = document.getElementById(`badge${port}`);
      const termEl = document.getElementById(`term${port}`);
      const leaderEl = document.getElementById(`leader${port}`);
      const pingEl = document.getElementById(`ping${port}`);

      try {
        const res = await fetchWithTimeout(`${nodeUrl}/raft/status`, {}, 1500);
        const duration = Math.round(performance.now() - start);

        if (res.ok) {
          const data = await res.json();
          const role = data.role || 'Follower';
          
          card.className = `node-card ${role.toLowerCase()}`;
          if (port === detectActivePort()) {
            card.classList.add('active-target');
          }

          badge.textContent = role;
          badge.className = `role-badge ${role.toLowerCase()}`;
          termEl.textContent = data.term != null ? data.term : '-';
          leaderEl.textContent = data.leader || 'None';
          pingEl.textContent = `${duration}ms`;
        } else {
          markNodeOffline(card, badge, termEl, leaderEl, pingEl);
        }
      } catch (err) {
        markNodeOffline(card, badge, termEl, leaderEl, pingEl);
      }
    }
  }

  function markNodeOffline(card, badge, termEl, leaderEl, pingEl) {
    card.className = 'node-card offline';
    if (card.dataset.port === detectActivePort()) {
      card.classList.add('active-target');
    }
    badge.textContent = 'Offline';
    badge.className = 'role-badge offline';
    termEl.textContent = '-';
    leaderEl.textContent = '-';
    pingEl.textContent = 'Timeout';
  }

  // Poll metrics from current target
  async function pollMetrics() {
    try {
      const res = await fetchWithTimeout(`${targetBaseUrl}/metrics`, {}, 1500);
      if (res.ok) {
        const m = await res.json();
        healthIndicator.style.background = 'var(--accent-emerald)';
        healthIndicator.style.boxShadow = '0 0 10px var(--accent-emerald)';
        healthText.textContent = `Connected (${detectActivePort()})`;

        statKeys.textContent = m.keys_count != null ? m.keys_count : '-';
        statWalIdx.textContent = m.wal_next_index != null ? m.wal_next_index : '-';
        statSets.textContent = m.total_sets != null ? m.total_sets : '0';
        statGets.textContent = m.total_gets != null ? m.total_gets : '0';
        keyCountBadge.textContent = `${m.keys_count || 0} keys`;
      }
    } catch {
      healthIndicator.style.background = 'var(--accent-rose)';
      healthIndicator.style.boxShadow = '0 0 10px var(--accent-rose)';
      healthText.textContent = `Offline (${detectActivePort()})`;
    }
  }

  // Poll key list from current target
  async function pollStoreList() {
    try {
      const res = await fetchWithTimeout(`${targetBaseUrl}/list`, {}, 2000);
      if (res.ok) {
        currentKeysMap = await res.json();
        renderTable(currentKeysMap);
      }
    } catch (err) {
      // Handled silently during polling
    }
  }

  // Render Table
  function renderTable(dataMap) {
    const filter = (searchKeyInput.value || '').toLowerCase().trim();
    const entries = Object.entries(dataMap || {});
    
    const filtered = entries.filter(([k, v]) => {
      if (!filter) return true;
      return k.toLowerCase().includes(filter) || String(v).toLowerCase().includes(filter);
    });

    if (filtered.length === 0) {
      dataTableBody.innerHTML = `
        <tr class="empty-row">
          <td colspan="3">${entries.length === 0 ? 'No keys in store yet. Use the form on the left to write one!' : 'No matching keys found.'}</td>
        </tr>
      `;
      return;
    }

    // Sort keys alphabetically
    filtered.sort((a, b) => a[0].localeCompare(b[0]));

    let html = '';
    for (const [k, v] of filtered) {
      const safeKey = escapeHtml(k);
      const safeVal = escapeHtml(typeof v === 'object' ? JSON.stringify(v) : String(v));
      html += `
        <tr>
          <td class="key-cell">${safeKey}</td>
          <td class="val-cell" title="${safeVal}">${safeVal}</td>
          <td style="text-align: right;">
            <button class="btn-delete" data-key="${safeKey}">Delete</button>
          </td>
        </tr>
      `;
    }
    dataTableBody.innerHTML = html;

    // Attach delete handlers
    dataTableBody.querySelectorAll('.btn-delete').forEach(btn => {
      btn.addEventListener('click', async () => {
        const key = btn.dataset.key;
        if (confirm(`Delete key "${key}"?`)) {
          await executeDelete(key);
        }
      });
    });
  }

  searchKeyInput.addEventListener('input', () => {
    renderTable(currentKeysMap);
  });

  // Execute SET
  setForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    const key = setKeyInput.value.trim();
    const val = setValInput.value.trim();
    if (!key || !val) return;

    logEvent('info', `Sending POST ${targetBaseUrl}/set {"key":"${key}"}`);
    try {
      const start = performance.now();
      const res = await fetch(`${targetBaseUrl}/set`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ key, val })
      });
      const dur = Math.round(performance.now() - start);
      const data = await res.json();

      if (res.ok) {
        logEvent('success', `SET success: key="${key}", wal_idx=${data.idx} (${dur}ms)`);
        setKeyInput.value = '';
        setValInput.value = '';
        refreshAll();
      } else {
        if (res.status === 403 && data.error === 'not leader') {
          logEvent('warn', `Write REJECTED: Target node is Follower! Leader is: ${data.leader}`);
          alert(`Write Rejected: This node is a Follower (Read Replica).\nCurrent Leader is: ${data.leader}`);
        } else {
          logEvent('error', `SET error ${res.status}: ${data.error || JSON.stringify(data)}`);
        }
      }
    } catch (err) {
      logEvent('error', `SET failed: ${err.message}`);
    }
  });

  // Test Follower Write Simulator
  testFollowerWriteBtn.addEventListener('click', async () => {
    logEvent('info', 'Searching for an active Follower in cluster...');
    let followerPort = null;
    for (const port of clusterPorts) {
      try {
        const res = await fetch(`http://localhost:${port}/raft/status`);
        if (res.ok) {
          const s = await res.json();
          if (s.role === 'Follower') {
            followerPort = port;
            break;
          }
        }
      } catch {}
    }

    if (!followerPort) {
      alert('No active followers found to test!');
      return;
    }

    logEvent('info', `Intentionally sending POST /set to Follower (: ${followerPort})...`);
    try {
      const res = await fetch(`http://localhost:${followerPort}/set`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ key: 'test_replica_write', val: 'blocked' })
      });
      const data = await res.json();
      if (res.status === 403) {
        logEvent('warn', `Follower (${followerPort}) rejected write with HTTP 403! Leader is: ${data.leader}`);
        alert(`Raft Follower Demonstration:\n\nFollower :${followerPort} correctly rejected the write!\nPayload: ${JSON.stringify(data)}\n\nThis proves Primary-Read Replica consensus is active!`);
      } else {
        logEvent('info', `Response: ${JSON.stringify(data)}`);
      }
    } catch (err) {
      logEvent('error', `Request error: ${err.message}`);
    }
  });

  // Execute GET
  getBtn.addEventListener('click', async () => {
    const key = getKeyInput.value.trim();
    if (!key) return;

    logEvent('info', `Querying GET ${targetBaseUrl}/get?key=${key}`);
    const start = performance.now();
    try {
      const res = await fetch(`${targetBaseUrl}/get?key=${encodeURIComponent(key)}`);
      const dur = Math.round(performance.now() - start);
      const text = await res.text();

      getResultBox.classList.remove('hidden');
      getTimeTaken.textContent = `${dur}ms`;

      if (res.ok) {
        getStatusCode.textContent = '200 OK';
        getStatusCode.className = 'status-code';
        try {
          const obj = JSON.parse(text);
          getResultContent.textContent = JSON.stringify(obj, null, 2);
        } catch {
          getResultContent.textContent = text;
        }
        logEvent('success', `GET "${key}" found (${dur}ms)`);
      } else {
        getStatusCode.textContent = `${res.status} Error`;
        getStatusCode.className = 'status-code error';
        getResultContent.textContent = text;
        logEvent('warn', `GET "${key}" returned ${res.status}`);
      }
    } catch (err) {
      getResultBox.classList.remove('hidden');
      getStatusCode.textContent = 'Network Error';
      getStatusCode.className = 'status-code error';
      getResultContent.textContent = err.message;
      logEvent('error', `GET failed: ${err.message}`);
    }
  });

  getKeyInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') getBtn.click();
  });

  // Execute DELETE
  async function executeDelete(key) {
    logEvent('info', `Sending POST ${targetBaseUrl}/del {"key":"${key}"}`);
    try {
      const res = await fetch(`${targetBaseUrl}/del`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ key })
      });
      const data = await res.json();
      if (res.ok) {
        logEvent('success', `Deleted key "${key}" (wal_idx: ${data.idx})`);
        refreshAll();
      } else {
        logEvent('error', `DEL error: ${data.error || JSON.stringify(data)}`);
        alert(`Delete failed: ${data.error || 'Check node status'}`);
      }
    } catch (err) {
      logEvent('error', `DEL error: ${err.message}`);
    }
  }

  // Trigger Snapshot
  triggerSnapshotBtn.addEventListener('click', async () => {
    logEvent('info', `Triggering POST ${targetBaseUrl}/snapshot`);
    try {
      const res = await fetch(`${targetBaseUrl}/snapshot`, { method: 'POST' });
      const data = await res.json();
      if (res.ok) {
        logEvent('success', 'Snapshot complete and WAL compacted!');
        alert('Snapshot created successfully! State safely compacted on disk.');
        refreshAll();
      } else {
        logEvent('error', `Snapshot error: ${data.error || JSON.stringify(data)}`);
        alert(`Snapshot failed: ${data.error || 'Requires leader permissions'}`);
      }
    } catch (err) {
      logEvent('error', `Snapshot request failed: ${err.message}`);
    }
  });

  // Save Snapshot Interval
  saveIntervalBtn.addEventListener('click', async () => {
    const secs = parseInt(snapshotIntervalInput.value, 10);
    if (isNaN(secs) || secs < 0) return;

    logEvent('info', `Updating snapshot interval to ${secs}s...`);
    try {
      const res = await fetch(`${targetBaseUrl}/config/snapshot`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ interval_secs: secs })
      });
      const data = await res.json();
      if (res.ok) {
        logEvent('success', `Snapshot interval updated to ${secs}s`);
      } else {
        logEvent('error', `Interval error: ${data.error}`);
      }
    } catch (err) {
      logEvent('error', `Interval request failed: ${err.message}`);
    }
  });

  // Refresh All
  async function refreshAll() {
    await Promise.all([
      pollClusterNodes(),
      pollMetrics(),
      pollStoreList()
    ]);
  }

  refreshBtn.addEventListener('click', () => {
    refreshAll();
    logEvent('info', 'Manual refresh triggered');
  });

  // Initial startup
  setTargetNode(targetBaseUrl);
  pollInterval = setInterval(refreshAll, 1500);

})();
