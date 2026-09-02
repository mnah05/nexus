// Nexus KV & Raft Cluster Client Engine v2
(function() {
  'use strict';

  // State
  let targetBaseUrl = window.location.origin;
  if (!targetBaseUrl || targetBaseUrl === 'null' || targetBaseUrl.includes('file:')) {
    targetBaseUrl = 'http://localhost:8001';
  }
  
  const clusterPorts = ['8001', '8002', '8003'];
  let currentKeysMap = {};
  let lastLeaderPort = null;

  // DOM Elements
  const segmentBtns = document.querySelectorAll('.segment-btn');
  const customTargetBtn = document.getElementById('customTargetBtn');
  const customInputWrapper = document.getElementById('customInputWrapper');
  const customInput = document.getElementById('customTargetInput');
  const applyCustomTargetBtn = document.getElementById('applyCustomTargetBtn');

  const livePill = document.getElementById('livePill');
  const latencyLabel = document.getElementById('latencyLabel');
  const refreshBtn = document.getElementById('refreshBtn');
  const terminalLog = document.getElementById('terminalLog');
  const clearConsoleBtn = document.getElementById('clearConsoleBtn');
  const quorumStatusBadge = document.getElementById('quorumStatusBadge');
  const toastRegion = document.getElementById('toastRegion');

  // Stats
  const statKeys = document.getElementById('statKeys');
  const statWalIdx = document.getElementById('statWalIdx');
  const statSets = document.getElementById('statSets');
  const statGets = document.getElementById('statGets');
  const keyCountBadge = document.getElementById('keyCountBadge');

  // Tabs
  const tabBtns = document.querySelectorAll('.tab-btn');
  const tabPanes = document.querySelectorAll('.tab-pane');
  const quickNewKeyBtn = document.getElementById('quickNewKeyBtn');

  // Forms
  const setForm = document.getElementById('setForm');
  const setKeyInput = document.getElementById('setKey');
  const setValInput = document.getElementById('setVal');
  const valTypeBadge = document.getElementById('valTypeBadge');
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
  function logEvent(badgeType, message) {
    const line = document.createElement('div');
    line.className = `term-line ${badgeType}`;
    const badgeText = badgeType.toUpperCase();
    line.innerHTML = `<span class="badge ${badgeType}">${escapeHtml(badgeText)}</span> ${escapeHtml(message)}`;
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

  function notify(type, title, message, duration = 5000) {
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.innerHTML = `
      <span class="toast-mark"></span>
      <div><div class="toast-title">${escapeHtml(title)}</div><div class="toast-message">${escapeHtml(message)}</div></div>
      <button class="toast-close" type="button" aria-label="Dismiss">x</button>
    `;
    const close = () => toast.remove();
    toast.querySelector('.toast-close').addEventListener('click', close);
    toastRegion.appendChild(toast);
    window.setTimeout(close, duration);
  }

  function askConfirmation(title, message, confirmLabel = 'Continue', danger = false) {
    return new Promise(resolve => {
      const backdrop = document.createElement('div');
      backdrop.className = 'confirm-backdrop';
      backdrop.innerHTML = `
        <div class="confirm-dialog" role="dialog" aria-modal="true" aria-labelledby="confirm-title">
          <h3 id="confirm-title">${escapeHtml(title)}</h3>
          <p>${escapeHtml(message)}</p>
          <div class="confirm-actions">
            <button class="confirm-cancel" type="button">Cancel</button>
            <button class="confirm-accept${danger ? ' danger' : ''}" type="button">${escapeHtml(confirmLabel)}</button>
          </div>
        </div>
      `;
      function onKeydown(event) {
        if (event.key === 'Escape') finish(false);
      }
      const finish = result => {
        document.removeEventListener('keydown', onKeydown);
        backdrop.remove();
        resolve(result);
      };
      backdrop.querySelector('.confirm-cancel').addEventListener('click', () => finish(false));
      backdrop.querySelector('.confirm-accept').addEventListener('click', () => finish(true));
      backdrop.addEventListener('click', event => {
        if (event.target === backdrop) finish(false);
      });
      document.addEventListener('keydown', onKeydown);
      document.body.appendChild(backdrop);
      backdrop.querySelector('.confirm-accept').focus();
    });
  }

  async function requestNodeAction(port, action) {
    try {
      const response = await fetchWithTimeout(`http://localhost:${port}/admin/${action}`, {
        method: 'POST'
      }, 1800);
      const data = await response.json();
      if (!response.ok) throw new Error(data.error || `HTTP ${response.status}`);
      return data;
    } catch (directError) {
      if (action !== 'restart') throw directError;

      // A stopped node cannot receive its own restart request. Ask a surviving
      // local node to relaunch it when running via make cluster-start.
      await new Promise(resolve => window.setTimeout(resolve, 900));
      try {
        const health = await fetchWithTimeout(`http://localhost:${port}/healthz`, {}, 700);
        if (health.ok) return { message: 'node restart accepted' };
      } catch {}

      let lastError = directError;
      for (const controlPort of clusterPorts) {
        if (controlPort === port) continue;
        try {
          const response = await fetchWithTimeout(`http://localhost:${controlPort}/admin/restart-peer`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ port })
          }, 1800);
          const data = await response.json();
          if (!response.ok) throw new Error(data.error || `HTTP ${response.status}`);
          return data;
        } catch (error) {
          lastError = error;
        }
      }
      throw lastError;
    }
  }

  function detectActivePort() {
    try {
      const u = new URL(targetBaseUrl);
      return u.port || (u.protocol === 'https:' ? '443' : '80');
    } catch {
      return '';
    }
  }

  // Set active target node
  function setTargetNode(url) {
    targetBaseUrl = url.replace(/\/+$/, '');
    const activePort = detectActivePort();
    
    segmentBtns.forEach(btn => {
      if (btn.dataset.node === activePort) {
        btn.classList.add('active');
      } else if (btn.dataset.node === 'custom' && !clusterPorts.includes(activePort)) {
        btn.classList.add('active');
      } else {
        btn.classList.remove('active');
      }
    });

    clusterPorts.forEach(port => {
      const card = document.getElementById(`cardNode${port}`);
      if (card) {
        if (port === activePort) {
          card.classList.add('targeted');
        } else {
          card.classList.remove('targeted');
        }
      }
    });

    logEvent('system', `Target changed to: ${targetBaseUrl}`);
    refreshAll();
  }

  // Segmented control click handlers
  segmentBtns.forEach(btn => {
    btn.addEventListener('click', () => {
      const nodeType = btn.dataset.node;
      if (nodeType === 'custom') {
        customInputWrapper.classList.toggle('hidden');
        if (!customInputWrapper.classList.contains('hidden')) {
          customInput.focus();
        }
      } else {
        customInputWrapper.classList.add('hidden');
        setTargetNode(`http://localhost:${nodeType}`);
      }
    });
  });

  applyCustomTargetBtn.addEventListener('click', () => {
    let val = customInput.value.trim();
    if (val) {
      if (!val.startsWith('http://') && !val.startsWith('https://')) {
        val = 'http://' + val;
      }
      setTargetNode(val);
      customInputWrapper.classList.add('hidden');
    }
  });

  // Clicking directly on node card selects it!
  clusterPorts.forEach(port => {
    const card = document.getElementById(`cardNode${port}`);
    if (card) {
      card.addEventListener('click', () => {
        setTargetNode(`http://localhost:${port}`);
      });
    }
  });

  // Local scenario controls for taking a node out of the cluster and bringing it back.
  document.querySelectorAll('.node-control-btn').forEach(button => {
    button.addEventListener('click', async (event) => {
      event.stopPropagation();

      const port = button.dataset.port;
      const action = button.dataset.nodeAction;
      const label = action === 'restart' ? 'restart' : 'stop';
      if (!await askConfirmation(`${label} node :${port}?`, 'This is a cluster scenario test.', label, action === 'shutdown')) return;

      button.disabled = true;
      logEvent('system', `Requesting ${label} for node :${port}...`);
      try {
        await requestNodeAction(port, action);
        logEvent('success', `Node :${port} ${action === 'restart' ? 'is restarting' : 'is shutting down'}.`);
        notify('success', `Node :${port} ${action}`, action === 'restart' ? 'The node is rejoining the cluster.' : 'The node is leaving the cluster.');
        setTimeout(refreshAll, 500);
      } catch (err) {
        logEvent('error', `Node :${port} ${label} failed: ${err.message}`);
        notify('error', `Node :${port} ${label} failed`, err.message);
        button.disabled = false;
      }
    });
  });

  // Tab Switching
  tabBtns.forEach(btn => {
    btn.addEventListener('click', () => {
      tabBtns.forEach(b => b.classList.remove('active'));
      tabPanes.forEach(p => p.classList.remove('active'));

      btn.classList.add('active');
      const targetPane = document.getElementById(btn.dataset.tab);
      if (targetPane) targetPane.classList.add('active');
    });
  });

  quickNewKeyBtn.addEventListener('click', () => {
    const setTabBtn = document.querySelector('[data-tab="tabSet"]');
    if (setTabBtn) setTabBtn.click();
    setKeyInput.focus();
  });

  // Value type detector
  setValInput.addEventListener('input', () => {
    const val = setValInput.value.trim();
    if (!val) {
      valTypeBadge.textContent = 'Auto (String / JSON)';
      return;
    }
    if ((val.startsWith('{') && val.endsWith('}')) || (val.startsWith('[') && val.endsWith(']'))) {
      try {
        JSON.parse(val);
        valTypeBadge.textContent = 'JSON Object';
        return;
      } catch {}
    }
    valTypeBadge.textContent = `String (${val.length} bytes)`;
  });

  clearConsoleBtn.addEventListener('click', () => {
    terminalLog.innerHTML = '';
  });

  // Keyboard shortcut: pressing / focuses search input
  window.addEventListener('keydown', (e) => {
    if (e.key === '/' && document.activeElement.tagName !== 'INPUT' && document.activeElement.tagName !== 'TEXTAREA') {
      e.preventDefault();
      searchKeyInput.focus();
    }
  });

  // Fetch with timeout helper
  async function fetchWithTimeout(url, options = {}, timeoutMs = 2500) {
    const controller = new AbortController();
    const id = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const response = await fetch(url, { ...options, signal: controller.signal });
      clearTimeout(id);
      return response;
    } catch (err) {
      clearTimeout(id);
      throw err;
    }
  }

  // Poll cluster nodes status
  async function pollClusterNodes() {
    let onlineCount = 0;
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
          onlineCount++;
          const data = await res.json();
          const role = data.role || 'Follower';

          card.className = `mesh-card ${role.toLowerCase()}`;
          if (port === detectActivePort()) {
            card.classList.add('targeted');
          }

          badge.textContent = role;
          badge.className = `role-pill ${role.toLowerCase()}`;
          termEl.textContent = data.term != null ? data.term : '-';

          let displayLeader = data.leader || 'None';
          if (displayLeader.includes('8001')) displayLeader = 'node-01 (:8001)';
          else if (displayLeader.includes('8002')) displayLeader = 'node-02 (:8002)';
          else if (displayLeader.includes('8003')) displayLeader = 'node-03 (:8003)';
          leaderEl.textContent = displayLeader;
          pingEl.textContent = `${duration}ms`;

          // If role changed to Leader, notify
          if (role === 'Leader' && lastLeaderPort !== port) {
            if (lastLeaderPort !== null) {
              logEvent('warn', `ELECTION: Node on :${port} was elected NEW LEADER for Term ${data.term}!`);
            }
            lastLeaderPort = port;
          }
        } else {
          markNodeOffline(card, badge, termEl, leaderEl, pingEl);
        }
      } catch {
        markNodeOffline(card, badge, termEl, leaderEl, pingEl);
      }
    }

    quorumStatusBadge.textContent = `${onlineCount}/3 nodes online`;
    if (onlineCount >= 2) {
      quorumStatusBadge.style.color = '#40d763';
      quorumStatusBadge.style.borderColor = 'rgba(64, 215, 99, 0.45)';
    } else {
      quorumStatusBadge.style.color = '#ff7082';
      quorumStatusBadge.style.borderColor = 'rgba(255, 112, 130, 0.45)';
    }
  }

  function markNodeOffline(card, badge, termEl, leaderEl, pingEl) {
    card.className = 'mesh-card offline';
    if (card.dataset.port === detectActivePort()) {
      card.classList.add('targeted');
    }
    badge.textContent = 'Offline';
    badge.className = 'role-pill offline';
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
        latencyLabel.textContent = `live :${detectActivePort()}`;
        latencyLabel.style.color = '#40d763';
        livePill.classList.remove('offline');

        statKeys.textContent = m.keys_count != null ? m.keys_count : '0';
        statWalIdx.textContent = m.wal_next_index != null ? m.wal_next_index : '1';
        statSets.textContent = (m.total_sets || 0) + (m.total_dels || 0);
        statGets.textContent = (m.total_gets || 0) + (m.total_lists || 0);
        keyCountBadge.textContent = `${m.keys_count || 0} keys`;
      }
    } catch {
      latencyLabel.textContent = `down :${detectActivePort()}`;
      latencyLabel.style.color = '#ff7082';
      livePill.classList.add('offline');
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
    } catch {}
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
        <tr class="empty-state-row">
          <td colspan="3">
            <div class="empty-state">
              <div class="empty-icon">
                <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8">
                  <circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/>
                </svg>
              </div>
              <p class="empty-title">${entries.length === 0 ? 'Store is currently empty' : 'No matching keys found'}</p>
              <span class="empty-sub">${entries.length === 0 ? 'Use the mutation form on the left to set a key!' : 'Try a different filter term'}</span>
            </div>
          </td>
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
          <td><span class="key-token">${safeKey}</span></td>
          <td><span class="val-token" title="${safeVal}">${safeVal}</span></td>
          <td style="text-align: right;">
            <button class="btn-row-del" data-key="${safeKey}">Delete</button>
          </td>
        </tr>
      `;
    }
    dataTableBody.innerHTML = html;

    // Attach delete listeners
    dataTableBody.querySelectorAll('.btn-row-del').forEach(btn => {
      btn.addEventListener('click', async () => {
        const key = btn.dataset.key;
        if (await askConfirmation(`Delete key "${key}"?`, 'This operation cannot be undone.', 'Delete', true)) {
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

    logEvent('system', `Dispatching write key="${key}" to ${targetBaseUrl}...`);
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
        logEvent('success', `WRITE COMMITTED: key="${key}" -> wal_idx=${data.idx} (${dur}ms). Replicated to followers!`);
        setKeyInput.value = '';
        setValInput.value = '';
        valTypeBadge.textContent = 'Auto (String / JSON)';
        refreshAll();
      } else {
        if (res.status === 403 && data.error === 'not leader') {
          logEvent('warn', `WRITE REJECTED: Target node is Follower! Active Leader is: ${data.leader}`);
          notify('warning', 'Write rejected by follower', `Current leader: ${data.leader}`);
        } else {
          logEvent('error', `Write Error (${res.status}): ${data.error || JSON.stringify(data)}`);
        }
      }
    } catch (err) {
      logEvent('error', `Network failure during write: ${err.message}`);
    }
  });

  // Follower Write Simulation
  testFollowerWriteBtn.addEventListener('click', async () => {
    logEvent('system', 'Scanning cluster for an active Follower replica...');
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
      notify('warning', 'No follower available', 'There are no active follower replicas right now.');
      return;
    }

    logEvent('warn', `Simulating client write to Follower (: ${followerPort})...`);
    try {
      const res = await fetch(`http://localhost:${followerPort}/set`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ key: 'demo_follower_write', val: 'blocked' })
      });
      const data = await res.json();
      if (res.status === 403) {
        logEvent('warn', `Follower (: ${followerPort}) rejected write with HTTP 403! Leader address returned: ${data.leader}`);
        notify('success', 'Follower protection verified', `Node :${followerPort} rejected the write with HTTP 403. Leader: ${data.leader || 'unknown'}.`, 7000);
      }
    } catch (err) {
      logEvent('error', `Demonstration failed: ${err.message}`);
    }
  });

  // Execute GET
  getBtn.addEventListener('click', async () => {
    const key = getKeyInput.value.trim();
    if (!key) return;

    logEvent('system', `GET lookup key="${key}" from ${targetBaseUrl}...`);
    const start = performance.now();
    try {
      const res = await fetch(`${targetBaseUrl}/get?key=${encodeURIComponent(key)}`);
      const dur = Math.round(performance.now() - start);
      const text = await res.text();

      getResultBox.classList.remove('hidden');
      getTimeTaken.textContent = `${dur}ms`;

      if (res.ok) {
        getStatusCode.textContent = '200 OK';
        getStatusCode.className = 'badge-status';
        try {
          const obj = JSON.parse(text);
          getResultContent.textContent = JSON.stringify(obj, null, 2);
        } catch {
          getResultContent.textContent = text;
        }
        logEvent('success', `GET "${key}" resolved in ${dur}ms`);
      } else {
        getStatusCode.textContent = `${res.status} Not Found`;
        getStatusCode.className = 'badge-status error';
        getResultContent.textContent = text;
        logEvent('warn', `GET "${key}" returned ${res.status}`);
      }
    } catch (err) {
      getResultBox.classList.remove('hidden');
      getStatusCode.textContent = 'Error';
      getStatusCode.className = 'badge-status error';
      getResultContent.textContent = err.message;
      logEvent('error', `GET failed: ${err.message}`);
    }
  });

  getKeyInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') getBtn.click();
  });

  // Execute DELETE
  async function executeDelete(key) {
    logEvent('system', `Dispatching delete for "${key}" to ${targetBaseUrl}...`);
    try {
      const res = await fetch(`${targetBaseUrl}/del`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ key })
      });
      const data = await res.json();
      if (res.ok) {
        logEvent('success', `DELETE COMMITTED: key="${key}" -> wal_idx=${data.idx}. Replicated to followers.`);
        refreshAll();
      } else {
        logEvent('error', `Delete Error (${res.status}): ${data.error}`);
        notify('error', 'Delete failed', data.error || 'The delete request was rejected.');
      }
    } catch (err) {
      logEvent('error', `Delete request failed: ${err.message}`);
    }
  }

  // Trigger Snapshot
  triggerSnapshotBtn.addEventListener('click', async () => {
    logEvent('system', `Triggering atomic snapshot & compaction on ${targetBaseUrl}...`);
    try {
      const res = await fetch(`${targetBaseUrl}/snapshot`, { method: 'POST' });
      const data = await res.json();
      if (res.ok) {
        logEvent('success', 'SNAPSHOT COMPLETED: In-memory state persisted, WAL truncated.');
        notify('success', 'Snapshot complete', 'State persisted and the WAL was compacted.');
        refreshAll();
      } else {
        logEvent('error', `Snapshot error: ${data.error}`);
        notify('error', 'Snapshot failed', data.error || 'The snapshot request was rejected.');
      }
    } catch (err) {
      logEvent('error', `Snapshot request failed: ${err.message}`);
    }
  });

  // Save Interval
  saveIntervalBtn.addEventListener('click', async () => {
    const secs = parseInt(snapshotIntervalInput.value, 10);
    if (isNaN(secs) || secs < 0) return;

    logEvent('system', `Updating automated snapshot interval to ${secs}s...`);
    try {
      const res = await fetch(`${targetBaseUrl}/config/snapshot`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ interval_secs: secs })
      });
      const data = await res.json();
      if (res.ok) {
        logEvent('success', `Snapshot interval updated to ${secs} seconds.`);
      } else {
        logEvent('error', `Interval update failed: ${data.error}`);
      }
    } catch (err) {
      logEvent('error', `Interval update failed: ${err.message}`);
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
    logEvent('system', 'Manual refresh triggered.');
  });

  // Sidebar navigation
  const navItems = document.querySelectorAll('.nav-item');
  navItems.forEach(item => {
    item.addEventListener('click', () => {
      navItems.forEach(i => i.classList.remove('active'));
      item.classList.add('active');
    });
  });

  // Startup
  setTargetNode(targetBaseUrl);
  setInterval(refreshAll, 1500);

})();
