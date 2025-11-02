let folders = [], mediaList = [], currentIndex = 0, target = '';
const query = new URLSearchParams(location.search);
const dir = query.get('dir') || '';

async function loadFolders() {
  const res = await fetch('/folders');
  folders = await res.json();
  const tree = buildTree(folders);
  renderTree(document.getElementById('folderList'), tree, true);
  renderTree(document.getElementById('targetFolder'), tree, false);
}

function buildTree(paths) {
  const root = {};
  for (const path of paths) {
    const parts = path.split('/').filter(Boolean);
    let node = root;
    for (let i = 0; i < parts.length; i++) {
      const part = parts[i];
      if (!node[part]) {
        node[part] = { _full: parts.slice(0, i + 1).join('/'), _children: {} };
      }
      node = node[part]._children;
    }
  }
  return root;
}

function renderTree(container, tree, isLeft) {
  container.innerHTML = '';
  function createNode(name, data) {
    const wrapper = document.createElement('div');
    const hasChildren = Object.keys(data._children).length > 0;
    const label = document.createElement('div');
    label.className = 'folder';
    label.textContent = name;
    const fullPath = data._full;
    label.dataset.full = fullPath;

    if (hasChildren) {
      label.classList.add('has-children');
      label.textContent = '';
      const arrow = document.createElement('span');
      arrow.className = 'arrow';
      arrow.textContent = '▸';
      arrow.onclick = e => {
        e.stopPropagation();
        wrapper.classList.toggle('expanded');
        arrow.textContent = wrapper.classList.contains('expanded') ? '▾' : '▸';
      };
      label.appendChild(arrow);
      label.appendChild(document.createTextNode(name));
    }

    if (isLeft && normalizePath(fullPath) === normalizePath(dir)) {
      label.classList.add('selected');
      expandAncestors(wrapper, container);
      setTimeout(() => label.scrollIntoView({ block: 'center' }), 50);
    }

    label.onclick = e => {
      e.stopPropagation();
      if (isLeft) {
        const url = new URL(location.href);
        url.searchParams.set('dir', fullPath);
        location.href = url.toString();
      } else {
        target = fullPath;
        const url = new URL(location.href);
        url.searchParams.set('target', target);
        window.history.replaceState({}, '', url.toString());
        document.querySelectorAll('#targetFolder .folder').forEach(el => el.classList.remove('selected'));
        label.classList.add('selected');
        setTimeout(() => label.scrollIntoView({ block: 'center' }), 50);
      }
    };

    wrapper.appendChild(label);

    if (hasChildren) {
      const nested = document.createElement('div');
      nested.className = 'nested';
      for (const key in data._children) nested.appendChild(createNode(key, data._children[key]));
      wrapper.appendChild(nested);
    }

    return wrapper;
  }

  for (const key in tree) container.appendChild(createNode(key, tree[key]));
}

function handleFolderClick(e, isLeft) {
  const label = e.target.closest('.folder');
  if (!label) return;
  e.stopPropagation();
  const fullPath = label.dataset.full;
  if (isLeft) {
    const url = new URL(location.href);
    url.searchParams.set('dir', fullPath);
    location.href = url.toString();
  } else {
    target = fullPath;
    const targetSearchInput = document.getElementById('targetSearch');
    if (targetSearchInput) {
      targetSearchInput.value = '';
      const tfolders = document.querySelectorAll('#targetFolder .folder');
      tfolders.forEach(f => { f.style.display = 'flex'; f.classList.remove('highlight'); });
      document.querySelectorAll('#targetFolder .expanded').forEach(el => el.classList.remove('expanded'));
    }
    const url = new URL(location.href);
    url.searchParams.set('target', target);
    window.history.replaceState({}, '', url.toString());
    document.querySelectorAll('#targetFolder .folder').forEach(el => el.classList.remove('selected'));
    label.classList.add('selected');
    setTimeout(() => label.scrollIntoView({ block: 'center' }), 50);
  }
}

function expandAncestors(el, container) {
  while (el && el !== container) {
    if (el.classList.contains('nested')) el.parentElement.classList.add('expanded');
    el = el.parentElement;
  }
}

function normalizePath(path) {
  return path ? path.replace(/^\/+|\/+$/g, '') : '';
}

async function loadMedia() {
  const res = await fetch(`/files?dir=${encodeURIComponent(dir)}`);
  mediaList = await res.json();
  currentIndex = 0;
  showCurrent();
}

function animateSwipe(direction) {
  const el = document.getElementById('preview');
  el.style.transform = '';
  el.classList.add(direction);
  setTimeout(() => {
    el.classList.remove(direction);
    showCurrent();
  }, 300);
}

function setupSwipeHandlers() {
  const preview = document.getElementById('preview');
  if (!preview) return;
  let dragging = false;
  let startX = 0;
  let pointerId = null;
  const threshold = 80;
  preview.addEventListener('pointerdown', function (e) {
    if (e.button && e.button !== 0) return;
    dragging = true;
    startX = e.clientX;
    pointerId = e.pointerId;
    preview.setPointerCapture(pointerId);
    preview.style.transition = 'none';
  });
  preview.addEventListener('pointermove', function (e) {
    if (!dragging || e.pointerId !== pointerId) return;
    const dx = e.clientX - startX;
    preview.style.transform = `translateX(${dx}px)`;
    const opacity = Math.max(0.4, 1 - Math.abs(dx) / 600);
    preview.style.opacity = opacity;
  });
  function finish(e) {
    if (!dragging) return;
    dragging = false;
    try { preview.releasePointerCapture(pointerId); } catch (ex) {}
    preview.style.transition = '';
    const dx = (e && typeof e.clientX === 'number') ? (e.clientX - startX) : 0;
    preview.style.transform = '';
    preview.style.opacity = '';
    if (dx > threshold) {
      accept();
      return;
    }
    if (dx < -threshold) {
      reject();
      return;
    }
  }
  preview.addEventListener('pointerup', finish);
  preview.addEventListener('pointercancel', finish);
  document.addEventListener('keydown', function (e) {
    if (e.key === 'ArrowLeft') {
      reject();
      return;
    }
    if (e.key === 'ArrowRight') {
      accept();
      return;
    }
  });
}

function showCurrent() {
  const preview = document.getElementById('preview');
  preview.className = '';
  if (currentIndex >= mediaList.length) {
    preview.innerHTML = '<h2>Done</h2>';
    return;
  }
  const item = mediaList[currentIndex];
  const ext = item.split('.').pop().toLowerCase();
  if (['jpg','jpeg','png','gif','webp','bmp'].includes(ext)) {
    preview.innerHTML = `<a data-fancybox="gallery" href="${item}"><img src="${item}" /></a>`;
    Fancybox.bind('[data-fancybox="gallery"]');
  } else if (['mp4','webm','mov','avi','mkv'].includes(ext)) {
    preview.innerHTML = `<a data-fancybox="gallery" href="${item}"><video src="${item}" autoplay muted loop></video></a>`;
    Fancybox.bind('[data-fancybox="gallery"]');
  } else {
    preview.innerHTML = `<div>${item}</div>`;
  }
}

async function accept() {
  if (!target) return alert('Select a target folder first');
  const fromPath = mediaList[currentIndex];
  await fetch('/move', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({ fromPath, targetFolder: target })
  });
  currentIndex++;
  animateSwipe('swipe-right');
}

function reject() {
  currentIndex++;
  animateSwipe('swipe-left');
}

function showAddFolderDialog() {
  document.getElementById("addFolderModal").style.display = "flex";
}
function hideAddFolderDialog() {
  document.getElementById("addFolderModal").style.display = "none";
  document.getElementById("folderName").value = "";
  document.getElementById("folderTarget").value = "";
  document.getElementById("addFolderMsg").innerText = "";
}
function showDeleteDialog() {
  const modal = document.getElementById('deleteConfirmModal');
  const msg = document.getElementById('deleteMsg');
  if (!modal || !msg) return;
  const item = mediaList[currentIndex] || '';
  msg.textContent = `Are you sure you want to delete ${item}?`;
  modal.style.display = 'flex';
}
function hideDeleteDialog() {
  const modal = document.getElementById('deleteConfirmModal');
  if (!modal) return;
  modal.style.display = 'none';
}
async function submitDelete() {
  const item = mediaList[currentIndex];
  if (!item) { hideDeleteDialog(); return; }
  try {
    const res = await fetch('/api/delete-file', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ fromPath: item })
    });
    if (res.ok) {
      mediaList.splice(currentIndex, 1);
      if (currentIndex >= mediaList.length) currentIndex = Math.max(0, mediaList.length - 1);
      hideDeleteDialog();
      showCurrent();
    } else {
      const txt = await res.text();
      const msg = document.getElementById('deleteMsg');
      if (msg) msg.textContent = txt || 'Delete failed';
    }
  } catch (e) {
    const msg = document.getElementById('deleteMsg');
    if (msg) msg.textContent = 'Request failed.';
  }
}
async function submitAddFolder() {
  const name = document.getElementById("folderName").value.trim();
  const target = document.getElementById("folderTarget").value.trim();
  if (!name) {
    document.getElementById("addFolderMsg").innerText = "Name is required.";
    return;
  }
  const body = target ? { name, target } : { name };
    try {
    const res = await fetch("/addfolder", {
      method: "POST",
      headers: {"Content-Type":"application/json"},
      body: JSON.stringify(body)
    });
    const txt = await res.text();
    if (res.ok) {
        document.getElementById("addFolderMsg").style.color = "#4caf50";
        document.getElementById("addFolderMsg").innerText = "Folder created.";
        hideAddFolderDialog();

        const newPath = target ? (normalizePath(target) ? `${normalizePath(target)}/${name}` : name) : name;
        const idx = folders.indexOf(newPath);
        if (idx === -1) folders.push(newPath);

        try {
          const tree = buildTree(folders);
          renderTree(document.getElementById('folderList'), tree, true);
          renderTree(document.getElementById('targetFolder'), tree, false);
          const container = document.getElementById('targetFolder');
          requestAnimationFrame(() => {
            const folderEls = container.querySelectorAll('.folder');
            for (const el of folderEls) {
              if (normalizePath(el.dataset.full) === normalizePath(newPath)) {
                el.classList.add('selected');
                expandAncestors(el.parentElement, container);
                setTimeout(() => el.scrollIntoView({ block: 'center' }), 100);
                break;
              }
            }
          });
        } catch (e) {
          console.warn('Failed to update folders in DOM after add:', e);
        }
    } else {
      document.getElementById("addFolderMsg").style.color = "#f44336";
      document.getElementById("addFolderMsg").innerText = txt;
    }
  } catch (e) {
    document.getElementById("addFolderMsg").style.color = "#f44336";
    document.getElementById("addFolderMsg").innerText = "Request failed.";
  }
}

function setupFolderSearch() {
  const searchInput = document.getElementById("folderSearch");
  if (!searchInput) return;
  searchInput.addEventListener("input", () => {
    const query = searchInput.value.trim().toLowerCase();
    const folders = document.querySelectorAll("#folderList .folder");
    if (!query) {
      folders.forEach(f => {
        f.style.display = "flex";
        f.classList.remove("highlight");
      });
      document.querySelectorAll("#folderList .expanded").forEach(el => el.classList.remove("expanded"));
      const selected = document.querySelector("#folderList .folder.selected");
      if (selected) expandAncestors(selected, document.getElementById("folderList"));
      return;
    }
    folders.forEach(f => {
      const name = f.textContent.toLowerCase();
      const match = name.includes(query);
      if (match) {
        f.style.display = "flex";
        f.classList.add("highlight");
        expandAncestors(f, document.getElementById("folderList"));
      } else {
        f.style.display = "none";
        f.classList.remove("highlight");
      }
    });
  });
}

function setupTargetSearch() {
  const searchInput = document.getElementById("targetSearch");
  if (!searchInput) return;
  searchInput.addEventListener("input", () => {
    const query = searchInput.value.trim().toLowerCase();
    const folders = document.querySelectorAll("#targetFolder .folder");
    if (!query) {
      folders.forEach(f => { f.style.display = "flex"; f.classList.remove("highlight"); });
      document.querySelectorAll("#targetFolder .expanded").forEach(el => el.classList.remove("expanded"));
      const selected = document.querySelector("#targetFolder .folder.selected");
      if (selected) expandAncestors(selected, document.getElementById('targetFolder'));
      return;
    }
    folders.forEach(f => {
      const name = f.textContent.toLowerCase();
      const match = name.includes(query);
      if (match) {
        f.style.display = "flex";
        f.classList.add("highlight");
        expandAncestors(f, document.getElementById('targetFolder'));
      } else {
        f.style.display = "none";
        f.classList.remove("highlight");
      }
    });
  });
}

async function init() {
  await loadFolders();
  setupFolderSearch();
  setupTargetSearch();
  setupWebsocket();
  
  const leftContainer = document.getElementById('folderList');
  if (leftContainer) leftContainer.addEventListener('click', e => handleFolderClick(e, true));
  const rightContainer = document.getElementById('targetFolder');
  if (rightContainer) rightContainer.addEventListener('click', e => handleFolderClick(e, false));
  setupSwipeHandlers();
  if (dir) await loadMedia();
  const targetParam = normalizePath(query.get('target'));
  if (targetParam) {
    const container = document.getElementById('targetFolder');
    requestAnimationFrame(() => {
      const folderEls = container.querySelectorAll('.folder');
      for (const el of folderEls) {
        if (normalizePath(el.dataset.full) === targetParam) {
          el.classList.add('selected');
          expandAncestors(el.parentElement, container);
          setTimeout(() => el.scrollIntoView({ block: 'center' }), 100);
          target = el.dataset.full;
          break;
        }
      }
    });
  }
}
init();

function setupWebsocket() {
  try {
    const proto = (location.protocol === 'https:') ? 'wss:' : 'ws:';
    let ws = null;
    let reconnectDelay = 1000;
    let reconnectTimer = null;

    function connect() {
      const url = proto + '//' + location.host + '/';
      console.log('WebSocket: connecting to', url);
      try {
        ws = new WebSocket(url);
      } catch (err) {
        console.warn('WebSocket: construction failed', err);
        scheduleReconnect();
        return;
      }

      ws.onopen = () => {
        console.log('WebSocket: open');
        reconnectDelay = 1000;
        try { ws.send(JSON.stringify({ type: 'subscribe', path: '' })); } catch (e) { console.warn('WS subscribe failed', e); }
      };

      ws.onmessage = (ev) => {
        try {
          const o = JSON.parse(ev.data);
          if (o && o.type === 'folder_added') {
            console.log('WebSocket: folder_added', o.path);
            loadFolders();
          }
        } catch (e) { console.warn('WS message parse error', e); }
      };

      ws.onclose = (ev) => {
        console.warn('WebSocket: closed', ev && ev.code, ev && ev.reason);
        scheduleReconnect();
      };

      ws.onerror = (ev) => {
        console.warn('WebSocket: error', ev);
      };
    }

    function scheduleReconnect() {
      if (reconnectTimer) return;
      reconnectTimer = setTimeout(() => {
        reconnectTimer = null;
        reconnectDelay = Math.min(30000, reconnectDelay * 1.5);
        connect();
      }, reconnectDelay);
      console.log('WebSocket: will reconnect in', reconnectDelay, 'ms');
    }

    connect();
  } catch (e) { console.warn('setupWebsocket failed', e); }
}

function goBack() {
  if (document.referrer) window.history.back();
  else window.location.href = '/';
}