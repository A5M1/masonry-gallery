let folders = [],
	mediaList = [],
	currentIndex = 0,
	target = "";

const DEBUG = true;
const log = (...args) => DEBUG && console.log("[mover]", ...args);
const warn = (...args) => console.warn("[mover]", ...args);
const error = (...args) => console.error("[mover]", ...args);

const query = new URLSearchParams(location.search);
const dir = query.get("dir") || "";
const videoExts = new Set(["mp4", "webm", "mov", "avi", "mkv"]);

function normalizePath(path) {
	return path ? path.replace(/^\/+|\/+$/g, "") : "";
}

function isVideoFile(path) {
	const ext = (path || "").split(".").pop().toLowerCase();
	return videoExts.has(ext);
}

function getVideoMime(ext) {
	switch (ext) {
		case "mp4": return "video/mp4";
		case "webm": return "video/webm";
		case "mov": return "video/quicktime";
		case "avi": return "video/x-msvideo";
		case "mkv": return "video/x-matroska";
		default: return "";
	}
}

async function loadFolders() {
	log("loadFolders()");
	const res = await fetch("/folders");
	folders = await res.json();
	log("folders loaded", folders.length);
	const tree = buildTree(folders);
	renderTree(document.getElementById("folderList"), tree, true);
	renderTree(document.getElementById("targetFolder"), tree, false);
}

function buildTree(paths) {
	const root = {};
	for (const path of paths) {
		const parts = path.split("/").filter(Boolean);
		let node = root;
		for (let i = 0; i < parts.length; i++) {
			const part = parts[i];
			if (!node[part]) {
				node[part] = {
					_full: parts.slice(0, i + 1).join("/"),
					_children: {}
				};
			}
			node = node[part]._children;
		}
	}
	return root;
}

function renderTree(container, tree, isLeft) {
	container.innerHTML = "";

	function createNode(name, data) {
		const wrapper = document.createElement("div");
		const hasChildren = Object.keys(data._children).length > 0;
		const label = document.createElement("div");
		label.className = "folder";
		label.textContent = name;
		const fullPath = data._full;
		label.dataset.full = fullPath;

		if (hasChildren) {
			label.classList.add("has-children");
			label.textContent = "";
			const arrow = document.createElement("span");
			arrow.className = "arrow";
			arrow.textContent = "▸";
			arrow.onclick = e => {
				e.stopPropagation();
				wrapper.classList.toggle("expanded");
				arrow.textContent = wrapper.classList.contains("expanded") ? "▾" : "▸";
			};
			label.appendChild(arrow);
			label.appendChild(document.createTextNode(name));
		}

		if (isLeft && normalizePath(fullPath) === normalizePath(dir)) {
			label.classList.add("selected");
			expandAncestors(wrapper, container);
			setTimeout(() => label.scrollIntoView({ block: "center" }), 50);
		}

		label.onclick = e => {
			e.stopPropagation();
			if (isLeft) {
				const url = new URL(location.href);
				url.searchParams.set("dir", fullPath);
				location.href = url.toString();
			} else {
				target = fullPath;
				const url = new URL(location.href);
				url.searchParams.set("target", target);
				window.history.replaceState({}, "", url.toString());
				document.querySelectorAll("#targetFolder .folder").forEach(el => el.classList.remove("selected"));
				label.classList.add("selected");
				setTimeout(() => label.scrollIntoView({ block: "center" }), 50);
				log("target selected", target);
			}
		};

		wrapper.appendChild(label);

		if (hasChildren) {
			const nested = document.createElement("div");
			nested.className = "nested";
			for (const key in data._children) nested.appendChild(createNode(key, data._children[key]));
			wrapper.appendChild(nested);
		}

		return wrapper;
	}

	for (const key in tree) container.appendChild(createNode(key, tree[key]));
}

function handleFolderClick(e, isLeft) {
	const label = e.target.closest(".folder");
	if (!label) return;
	e.stopPropagation();
	const fullPath = label.dataset.full;

	if (isLeft) {
		log("left folder click", fullPath);
		const url = new URL(location.href);
		url.searchParams.set("dir", fullPath);
		location.href = url.toString();
	} else {
		target = fullPath;
		log("right folder click", fullPath);

		const targetSearchInput = document.getElementById("targetSearch");
		if (targetSearchInput) {
			targetSearchInput.value = "";
			const tfolders = document.querySelectorAll("#targetFolder .folder");
			tfolders.forEach(f => {
				f.style.display = "flex";
				f.classList.remove("highlight");
			});
			document.querySelectorAll("#targetFolder .expanded").forEach(el => el.classList.remove("expanded"));
		}

		const url = new URL(location.href);
		url.searchParams.set("target", target);
		window.history.replaceState({}, "", url.toString());

		document.querySelectorAll("#targetFolder .folder").forEach(el => el.classList.remove("selected"));
		label.classList.add("selected");
		setTimeout(() => label.scrollIntoView({ block: "center" }), 50);
	}
}

function expandAncestors(el, container) {
	while (el && el !== container) {
		if (el.classList.contains("nested")) el.parentElement.classList.add("expanded");
		el = el.parentElement;
	}
}

async function loadMedia() {
	log("loadMedia()", { dir });
	const res = await fetch(`/files?dir=${encodeURIComponent(dir)}`);
	mediaList = await res.json();
	currentIndex = 0;
	log("media loaded", mediaList.length);
	showCurrent();
}

function animateSwipe(direction) {
	const el = document.getElementById("preview");
	el.style.transform = "";
	el.classList.add(direction);
	setTimeout(() => {
		el.classList.remove(direction);
		showCurrent();
	}, 300);
}
function ensureFancyboxVideoHolder(src, mime) {
	let holder = document.getElementById("fancybox-video-holder");
	if (!holder) {
		holder = document.createElement("div");
		holder.id = "fancybox-video-holder";
		holder.style.display = "none";
		holder.className="";
		document.body.appendChild(holder);
	}

	holder.innerHTML = `
		<video
			class="f-html5video"
			autoplay
			muted
			loop
			playsinline
			controls
			preload="auto"

		>
			<source src="${src}" type="${mime}">
			Your browser doesn't support HTML5 video.
		</video>
	`;

	return holder;
}

function openMedia(src) {
	const ext = (src || "").split(".").pop().toLowerCase();
	const video = videoExts.has(ext);

	log("openMedia()", { src, ext, video });

	if (!video) {
		Fancybox.show([{ src, type: "image" }], {
			Animated: false,
			showClass: false,
			hideClass: false,
			Toolbar: { display: { left: [], middle: [], right: ["close"] } }
		});
		return;
	}

	const mime = getVideoMime(ext);
	const holder = ensureFancyboxVideoHolder(src, mime);

	Fancybox.show([{
		src: "#fancybox-video-holder",
		type: "inline",
	}], {
		Animated: false,
		showClass: false,
		hideClass: false,
		Toolbar: { display: { left: [], middle: [], right: ["close"] } },
		on: {
			done: () => {
				setTimeout(() => {
					const videoEl = document.getElementById("fancyboxVideoPlayer");
					if (!videoEl) return;

					videoEl.style.maxWidth = "100%";
					videoEl.style.maxHeight = "80vh";
					videoEl.style.width = "auto";
					videoEl.style.height = "auto";
					videoEl.loop = true;
					videoEl.muted = true;
					videoEl.autoplay = true;
					videoEl.playsInline = true;
					videoEl.controls = true;
					videoEl.load();
					videoEl.play().catch(err => error("play() rejected", { src, err }));
				}, 0);
			}
		}
	});
}
function setupSwipeHandlers() {
	const preview = document.getElementById("preview");
	if (!preview) return;

	let dragging = false;
	let startX = 0;
	let startY = 0;
	let pointerId = null;
	const threshold = 80;

	preview.addEventListener("pointerdown", function (e) {
		if (e.button && e.button !== 0) return;
		dragging = true;
		startX = e.clientX;
		startY = e.clientY;
		pointerId = e.pointerId;
		preview.setPointerCapture(pointerId);
		preview.style.transition = "none";
		log("pointerdown", { startX, startY, pointerId });
	});

	preview.addEventListener("pointermove", function (e) {
		if (!dragging || e.pointerId !== pointerId) return;
		const dx = e.clientX - startX;
		preview.style.transform = `translateX(${dx}px)`;
		preview.style.opacity = Math.max(0.4, 1 - Math.abs(dx) / 600);
	});

	function finish(e) {
		if (!dragging) return;
		dragging = false;

		try {
			preview.releasePointerCapture(pointerId);
		} catch (ex) {
			warn("releasePointerCapture failed", ex);
		}

		preview.style.transition = "";

		const dx = e && typeof e.clientX === "number" ? e.clientX - startX : 0;
		const dy = e && typeof e.clientY === "number" ? e.clientY - startY : 0;

		preview.style.transform = "";
		preview.style.opacity = "";

		log("finish()", { dx, dy, currentIndex, item: mediaList[currentIndex] });

		if (Math.abs(dx) < 5 && Math.abs(dy) < 5) {
			const item = mediaList[currentIndex];
			if (!item) {
				warn("No item to open");
				return;
			}
			openMedia(item);
			return;
		}

		if (dx > threshold) {
			log("swipe accept");
			accept();
			return;
		}

		if (dx < -threshold) {
			log("swipe reject");
			reject();
			return;
		}

		log("gesture ignored");
	}

	preview.addEventListener("pointerup", finish);
	preview.addEventListener("pointercancel", finish);

	document.addEventListener("keydown", function (e) {
		if (e.key === "ArrowLeft") {
			reject();
			return;
		}
		if (e.key === "ArrowRight") {
			accept();
			return;
		}
	});
}

function showCurrent() {
	const preview = document.getElementById("preview");
	preview.className = "";

	if (currentIndex >= mediaList.length) {
		preview.innerHTML = "<h2>Done</h2>";
		log("showCurrent done");
		return;
	}

	const item = mediaList[currentIndex];
	const ext = item.split(".").pop().toLowerCase();

	log("showCurrent()", { currentIndex, item, ext });

	if (["jpg", "jpeg", "png", "gif", "webp", "bmp"].includes(ext)) {
		preview.innerHTML = `<img src="${item}" style="cursor:pointer;" />`;
	} else if (isVideoFile(item)) {
		preview.innerHTML = `
			<video id="previewVideo" autoplay muted loop playsinline  style="cursor:pointer; max-width:100%; max-height:100%;">
				<source src="${item}" type="${getVideoMime(ext)}">
			</video>`;
		const video = document.getElementById("previewVideo");
		if (video) {
			video.addEventListener("loadedmetadata", () => log("preview loadedmetadata", { item, duration: video.duration }));
			video.addEventListener("canplay", () => log("preview canplay", { item, readyState: video.readyState }));
			video.addEventListener("play", () => log("preview play", { item }));
			video.addEventListener("pause", () => log("preview pause", { item }));
			video.addEventListener("ended", () => log("preview ended", { item, loop: video.loop }));
			video.addEventListener("error", () => error("preview error", { item, error: video.error }));
			video.play().catch(err => warn("preview play rejected", { item, err }));
		}
	} else {
		preview.innerHTML = `<div>${item}</div>`;
	}
}

async function accept() {
	if (!target) return alert("Select a target folder first");
	const fromPath = mediaList[currentIndex];
	log("accept()", { fromPath, target });

	await fetch("/move", {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify({ fromPath, targetFolder: target })
	});

	currentIndex++;
	animateSwipe("swipe-right");
}

function reject() {
	log("reject()", { currentIndex });
	currentIndex++;
	animateSwipe("swipe-left");
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
	const modal = document.getElementById("deleteConfirmModal");
	const msg = document.getElementById("deleteMsg");
	if (!modal || !msg) return;
	const item = mediaList[currentIndex] || "";
	msg.textContent = `Are you sure you want to delete ${item}?`;
	modal.style.display = "flex";
}

function hideDeleteDialog() {
	const modal = document.getElementById("deleteConfirmModal");
	if (!modal) return;
	modal.style.display = "none";
}

async function submitDelete() {
	const item = mediaList[currentIndex];
	if (!item) {
		hideDeleteDialog();
		return;
	}

	log("submitDelete()", { item });

	try {
		const res = await fetch("/api/delete-file", {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ fromPath: item })
		});

		if (res.ok) {
			mediaList.splice(currentIndex, 1);
			if (currentIndex >= mediaList.length) currentIndex = Math.max(0, mediaList.length - 1);
			hideDeleteDialog();
			showCurrent();
		} else {
			const txt = await res.text();
			const msg = document.getElementById("deleteMsg");
			if (msg) msg.textContent = txt || "Delete failed";
			warn("delete failed", txt);
		}
	} catch (e) {
		const msg = document.getElementById("deleteMsg");
		if (msg) msg.textContent = "Request failed.";
		error("delete request error", e);
	}
}

async function submitAddFolder() {
	const name = document.getElementById("folderName").value.trim();
	const targetInput = document.getElementById("folderTarget").value.trim();

	if (!name) {
		document.getElementById("addFolderMsg").innerText = "Name is required.";
		return;
	}

	const body = targetInput ? { name, target: targetInput } : { name };

	log("submitAddFolder()", body);

	try {
		const res = await fetch("/addfolder", {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify(body)
		});
		const txt = await res.text();

		if (res.ok) {
			document.getElementById("addFolderMsg").style.color = "#4caf50";
			document.getElementById("addFolderMsg").innerText = "Folder created.";
			hideAddFolderDialog();

			const newPath = targetInput
				? normalizePath(targetInput)
					? `${normalizePath(targetInput)}/${name}`
					: name
				: name;

			if (folders.indexOf(newPath) === -1) folders.push(newPath);

			// Update the active target logic
			target = newPath;
			const url = new URL(location.href);
			url.searchParams.set("target", target);
			window.history.replaceState({}, "", url.toString());
			log("target updated to new folder", target);

			try {
				const tree = buildTree(folders);
				renderTree(document.getElementById("folderList"), tree, true);
				renderTree(document.getElementById("targetFolder"), tree, false);

				const container = document.getElementById("targetFolder");
				requestAnimationFrame(() => {
					// Clear previous selections first
					container.querySelectorAll(".folder").forEach(el => el.classList.remove("selected"));
					
					const folderEls = container.querySelectorAll(".folder");
					for (const el of folderEls) {
						if (normalizePath(el.dataset.full) === normalizePath(newPath)) {
							el.classList.add("selected");
							expandAncestors(el.parentElement, container);
							setTimeout(() => el.scrollIntoView({ block: "center" }), 100);
							break;
						}
					}
				});
			} catch (e) {
				warn("Failed to update folders in DOM after add:", e);
			}
		} else {
			document.getElementById("addFolderMsg").style.color = "#f44336";
			document.getElementById("addFolderMsg").innerText = txt;
		}
	} catch (e) {
		document.getElementById("addFolderMsg").style.color = "#f44336";
		document.getElementById("addFolderMsg").innerText = "Request failed.";
		error("add folder request error", e);
	}
}
function setupFolderSearch() {
	const searchInput = document.getElementById("folderSearch");
	if (!searchInput) return;

	searchInput.addEventListener("input", () => {
		const queryText = searchInput.value.trim().toLowerCase();
		const folders = document.querySelectorAll("#folderList .folder");

		if (!queryText) {
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
			const match = name.includes(queryText);
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
		const queryText = searchInput.value.trim().toLowerCase();
		const folders = document.querySelectorAll("#targetFolder .folder");

		if (!queryText) {
			folders.forEach(f => {
				f.style.display = "flex";
				f.classList.remove("highlight");
			});
			document.querySelectorAll("#targetFolder .expanded").forEach(el => el.classList.remove("expanded"));
			const selected = document.querySelector("#targetFolder .folder.selected");
			if (selected) expandAncestors(selected, document.getElementById("targetFolder"));
			return;
		}

		folders.forEach(f => {
			const name = f.textContent.toLowerCase();
			const match = name.includes(queryText);
			if (match) {
				f.style.display = "flex";
				f.classList.add("highlight");
				expandAncestors(f, document.getElementById("targetFolder"));
			} else {
				f.style.display = "none";
				f.classList.remove("highlight");
			}
		});
	});
}

function setupWebsocket() {
	try {
		const proto = location.protocol === "https:" ? "wss:" : "ws:";
		let ws = null;
		let reconnectDelay = 1000;
		let reconnectTimer = null;

		function connect() {
			const url = proto + "//" + location.host + "/";
			log("WebSocket connecting", url);

			try {
				ws = new WebSocket(url);
			} catch (err) {
				warn("WebSocket construction failed", err);
				scheduleReconnect();
				return;
			}

			ws.onopen = () => {
				log("WebSocket open");
				reconnectDelay = 1000;
				try {
					ws.send(JSON.stringify({ type: "subscribe", path: "" }));
					log("WebSocket subscribed");
				} catch (e) {
					warn("WS subscribe failed", e);
				}
			};

			ws.onmessage = ev => {
				try {
					const o = JSON.parse(ev.data);
					if (!o) return;
					log("WS message", o);

					if (o.type === "folder_added" || o.type === "folderAdded") {
						const rawPath = o.path || "";
						const newPath = rawPath.replace(/\\/g, "/");
						log("folder added event", { rawPath, newPath });

						loadFolders()
							.then(() => {
								try {
									const container = document.getElementById("targetFolder");
									if (!container) return;
									const folderEls = container.querySelectorAll(".folder");
									const normNew = normalizePath(newPath);
									const newLast = normNew && normNew.split("/").length ? normNew.split("/").pop() : "";

									for (const el of folderEls) {
										const elp = normalizePath(el.dataset.full || "");
										if (elp === normNew || (newLast && elp.split("/").pop() === newLast)) {
											document.querySelectorAll("#targetFolder .folder").forEach(ele => ele.classList.remove("selected"));
											el.classList.add("selected");
											expandAncestors(el.parentElement, container);
											setTimeout(() => el.scrollIntoView({ block: "center" }), 100);
											target = el.dataset.full;
											const url = new URL(location.href);
											url.searchParams.set("target", target);
											window.history.replaceState({}, "", url.toString());
											break;
										}
									}
								} catch (e) {
									warn("Failed to select new folder after WS event", e);
								}
							})
							.catch(err => warn("loadFolders after WS failed", err));
					}
				} catch (e) {
					warn("WS message parse error", e);
				}
			};

			ws.onclose = ev => {
				warn("WebSocket closed", ev && ev.code, ev && ev.reason);
				scheduleReconnect();
			};

			ws.onerror = ev => {
				warn("WebSocket error", ev);
			};
		}

		function scheduleReconnect() {
			if (reconnectTimer) return;
			reconnectTimer = setTimeout(() => {
				reconnectTimer = null;
				reconnectDelay = Math.min(30000, reconnectDelay * 1.5);
				connect();
			}, reconnectDelay);
			log("WebSocket reconnect scheduled", reconnectDelay);
		}

		connect();
	} catch (e) {
		warn("setupWebsocket failed", e);
	}
}

function goBack() {
	if (document.referrer) window.history.back();
	else window.location.href = "/";
}

async function init() {
	await loadFolders();
	setupFolderSearch();
	setupTargetSearch();
	setupWebsocket();

	const leftContainer = document.getElementById("folderList");
	if (leftContainer) leftContainer.addEventListener("click", e => handleFolderClick(e, true));

	const rightContainer = document.getElementById("targetFolder");
	if (rightContainer) rightContainer.addEventListener("click", e => handleFolderClick(e, false));

	setupSwipeHandlers();

	if (dir) await loadMedia();

	const targetParam = normalizePath(query.get("target"));
	if (targetParam) {
		const container = document.getElementById("targetFolder");
		requestAnimationFrame(() => {
			const folderEls = container.querySelectorAll(".folder");
			for (const el of folderEls) {
				if (normalizePath(el.dataset.full) === targetParam) {
					el.classList.add("selected");
					expandAncestors(el.parentElement, container);
					setTimeout(() => el.scrollIntoView({ block: "center" }), 100);
					target = el.dataset.full;
					log("target restored from URL", target);
					break;
				}
			}
		});
	}
}

init();