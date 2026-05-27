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
		case "mp4":
			return "video/mp4";
		case "webm":
			return "video/webm";
		case "mov":
			return "video/quicktime";
		case "avi":
			return "video/x-msvideo";
		case "mkv":
			return "video/x-matroska";
		default:
			return "";
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
	let displayTree = {};

	if (isLeft) {
		displayTree = tree;
	} else {
		const counts = {};
		const sortedKeys = Object.keys(tree).sort((a, b) =>
			a.localeCompare(b, undefined, {sensitivity: "base"})
		);

		for (const key of sortedKeys) {
			const firstChar = key.trim().charAt(0).toUpperCase();
			const letterGroup =
				firstChar >= "A" && firstChar <= "Z" ? firstChar : "#";
			counts[letterGroup] = (counts[letterGroup] || 0) + 1;
		}

		for (const key of sortedKeys) {
			const firstChar = key.trim().charAt(0).toUpperCase();
			const letterGroup =
				firstChar >= "A" && firstChar <= "Z" ? firstChar : "#";

			if (counts[letterGroup] > 1) {
				if (!displayTree[letterGroup]) {
					displayTree[letterGroup] = {
						_virtual: true,
						_children: {}
					};
				}
				displayTree[letterGroup]._children[key] = tree[key];
			} else {
				displayTree[key] = tree[key];
			}
		}
	}

	function createNode(name, data) {
		const wrapper = document.createElement("div");
		const childrenKeys = Object.keys(data._children || {}).sort((a, b) =>
			a.localeCompare(b, undefined, {sensitivity: "base"})
		);
		const hasChildren = childrenKeys.length > 0;
		const label = document.createElement("div");
		label.className = "folder";
		label.textContent = name;

		if (data._virtual) {
			label.classList.add("virtual-letter");
		} else {
			label.dataset.full = data._full;
		}

		if (hasChildren) {
			label.classList.add("has-children");
			label.textContent = "";
			const arrow = document.createElement("span");
			arrow.className = "arrow";
			arrow.textContent = "▸";
			arrow.onclick = e => {
				e.stopPropagation();
				wrapper.classList.toggle("expanded");
				arrow.textContent =
					wrapper.classList.contains("expanded") ? "▾" : "▸";
			};
			label.appendChild(arrow);
			label.appendChild(document.createTextNode(name));
		}

		const currentTarget = normalizePath(
			new URLSearchParams(location.search).get("target") || target
		);
		if (!data._virtual) {
			if (isLeft && normalizePath(data._full) === normalizePath(dir)) {
				label.classList.add("selected");
				expandAncestors(wrapper, container);
				setTimeout(() => label.scrollIntoView({block: "center"}), 50);
			} else if (!isLeft && normalizePath(data._full) === currentTarget) {
				label.classList.add("selected");
				expandAncestors(wrapper, container);
				setTimeout(() => label.scrollIntoView({block: "center"}), 50);
			}
		}

		wrapper.appendChild(label);

		if (hasChildren) {
			const nested = document.createElement("div");
			nested.className = "nested";
			for (const key of childrenKeys) {
				nested.appendChild(createNode(key, data._children[key]));
			}
			wrapper.appendChild(nested);
		}

		return wrapper;
	}

	const sortedRootKeys = Object.keys(displayTree).sort((a, b) =>
		a.localeCompare(b, undefined, {sensitivity: "base"})
	);
	for (const key of sortedRootKeys) {
		container.appendChild(createNode(key, displayTree[key]));
	}
}

function handleFolderClick(e, isLeft) {
	const label = e.target.closest(".folder");
	if (!label || label.classList.contains("virtual-letter")) return;
	e.stopPropagation();
	const fullPath = label.dataset.full;

	if (isLeft) {
		log("left folder click", fullPath);
		document
			.querySelectorAll("#folderList .folder")
			.forEach(el => el.classList.remove("selected"));
		label.classList.add("selected");
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
		}

		const targetContainer = document.getElementById("targetFolder");
		document.querySelectorAll("#targetFolder .expanded").forEach(el => {
			el.classList.remove("expanded");
			const arrow = el.querySelector(".arrow");
			if (arrow) arrow.textContent = "▸";
		});

		document
			.querySelectorAll("#targetFolder .folder")
			.forEach(el => el.classList.remove("selected"));
		label.classList.add("selected");
		
		expandAncestors(label.parentElement, targetContainer);
		expandToPath(label, targetContainer);
		
		const url = new URL(location.href);
		url.searchParams.set("target", target);
		window.history.replaceState({}, "", url.toString());

		setTimeout(() => label.scrollIntoView({block: "center"}), 50);
	}
}

function expandToPath(folderElement, container) {
	const fullPath = folderElement.querySelector(".folder").dataset.full;
	if (!fullPath) return;
	
	const parts = fullPath.split("/").filter(Boolean);
	let current = container;
	
	for (let i = 0; i < parts.length; i++) {
		const folders = current.querySelectorAll(":scope > div > .folder");
		for (const folder of folders) {
			const dataFull = folder.dataset.full;
			if (dataFull && dataFull === parts.slice(0, i + 1).join("/")) {
				const wrapper = folder.parentElement;
				if (wrapper && wrapper.classList.contains("nested")) {
					wrapper.parentElement.classList.add("expanded");
				}
				current = wrapper;
				break;
			}
		}
	}
}

function expandAncestors(el, container) {
	while (el && el !== container) {
		if (el.classList.contains("nested"))
			el.parentElement.classList.add("expanded");
		el = el.parentElement;
	}
}

async function loadMedia() {
	log("loadMedia()", {dir});
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

function openMedia(src) {
	const ext = (src || "").split(".").pop().toLowerCase();
	const isVideo = videoExts.has(ext);

	if (!isVideo) {
		Fancybox.show([{src, type: "image"}], {
			Animated: false,
			showClass: false,
			hideClass: false,
			Toolbar: {display: {left: [], middle: [], right: ["close"]}}
		});
		return;
	}

	/*
	const html5videoTpl = `<video class="f-html5video" playsinline controls controlsList="nodownload" poster="" muted autoplay loop><source src="{{src}}" type="{{format}}" />Sorry, your browser doesn't support embedded videos.</video>`;
	Fancybox.show([{src, type: "html5video", html5videoFormat: getVideoMime(ext)}], {
		Animated: false,
		showClass: false,
		hideClass: false,
		Toolbar: {display: {left: [], middle: [], right: ["close"]}},
		Video: {autoplay: true, html5videoTpl}
	});
	*/
	$.fancybox.open([
		{
			src,
			type: "video"
		}
	], {
		buttons: ["close"],
		loop: true,
		video: {
			autoplay: true,
			preload: "metadata",
			controls: true,
			loop: true
		},
		afterShow: function (instance, current) {
			const video = current.$content.find("video").get(0);
			if (video) {
				Object.assign(video, {
					controls: true,
					preload: "metadata",
					muted: true,
					loop: true
				});
				const playPromise = video.play();
				if (playPromise && typeof playPromise.then === "function") {
					playPromise.catch(() => {
						video.muted = true;
						video.play();
					});
				}
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
		log("pointerdown", {startX, startY, pointerId});
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

		log("finish()", {dx, dy, currentIndex, item: mediaList[currentIndex]});

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

	log("showCurrent()", {currentIndex, item, ext});

	if (["jpg", "jpeg", "png", "gif", "webp", "bmp"].includes(ext)) {
		preview.innerHTML = `<img src="${item}" style="cursor:pointer;" />`;
	} else if (isVideoFile(item)) {
		preview.innerHTML = `
			<video id="previewVideo" autoplay muted loop playsinline style="cursor:pointer; max-width:100%; max-height:100%;">
				<source src="${item}" type="${getVideoMime(ext)}">
			</video>`;
		const video = document.getElementById("previewVideo");
		if (video) {
			video.addEventListener("loadedmetadata", () =>
				log("preview loadedmetadata", {item, duration: video.duration})
			);
			video.addEventListener("canplay", () =>
				log("preview canplay", {item, readyState: video.readyState})
			);
			video.addEventListener("play", () => log("preview play", {item}));
			video.addEventListener("pause", () => log("preview pause", {item}));
			video.addEventListener("ended", () =>
				log("preview ended", {item, loop: video.loop})
			);
			video.addEventListener("error", () =>
				error("preview error", {item, error: video.error})
			);
			video
				.play()
				.catch(err => warn("preview play rejected", {item, err}));
		}
	} else {
		preview.innerHTML = `<div>${item}</div>`;
	}
}

async function accept() {
	if (!target) return alert("Select a target folder first");
	const fromPath = mediaList[currentIndex];
	log("accept()", {fromPath, target});

	await fetch("/move", {
		method: "POST",
		headers: {"Content-Type": "application/json"},
		body: JSON.stringify({fromPath, targetFolder: target})
	});

	currentIndex++;
	animateSwipe("swipe-right");
}

function reject() {
	log("reject()", {currentIndex});
	currentIndex++;
	animateSwipe("swipe-left");
}

function showAddFolderDialog() {
	document.getElementById("addFolderModal").classList.add("show");
}

function hideAddFolderDialog() {
	document.getElementById("addFolderModal").classList.remove("show");
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
	modal.classList.add("show");
}

function hideDeleteDialog() {
	const modal = document.getElementById("deleteConfirmModal");
	if (!modal) return;
	modal.classList.remove("show");
}

async function submitDelete() {
	const item = mediaList[currentIndex];
	if (!item) {
		hideDeleteDialog();
		return;
	}

	log("submitDelete()", {item});

	try {
		const res = await fetch("/api/delete-file", {
			method: "POST",
			headers: {"Content-Type": "application/json"},
			body: JSON.stringify({fromPath: item})
		});

		if (res.ok) {
			mediaList.splice(currentIndex, 1);
			if (currentIndex >= mediaList.length)
				currentIndex = Math.max(0, mediaList.length - 1);
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

	const body = targetInput ? {name, target: targetInput} : {name};

	log("submitAddFolder()", body);

	try {
		const res = await fetch("/addfolder", {
			method: "POST",
			headers: {"Content-Type": "application/json"},
			body: JSON.stringify(body)
		});
		const txt = await res.text();

		if (res.ok) {
			document.getElementById("addFolderMsg").style.color = "#4caf50";
			document.getElementById("addFolderMsg").innerText =
				"Folder created.";
			hideAddFolderDialog();

			const newPath =
				targetInput ?
					normalizePath(targetInput) ?
						`${normalizePath(targetInput)}/${name}`
					:	name
				:	name;

			if (folders.indexOf(newPath) === -1) folders.push(newPath);

			target = newPath;
			const url = new URL(location.href);
			url.searchParams.set("target", target);
			window.history.replaceState({}, "", url.toString());
			log("target updated to new folder", target);

			try {
				const tree = buildTree(folders);
				renderTree(document.getElementById("folderList"), tree, true);
				renderTree(
					document.getElementById("targetFolder"),
					tree,
					false
				);
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
			document
				.querySelectorAll("#folderList .expanded")
				.forEach(el => el.classList.remove("expanded"));
			const selected = document.querySelector(
				"#folderList .folder.selected"
			);
			if (selected)
				expandAncestors(
					selected,
					document.getElementById("folderList")
				);
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
			document
				.querySelectorAll("#targetFolder .expanded")
				.forEach(el => el.classList.remove("expanded"));
			const selected = document.querySelector(
				"#targetFolder .folder.selected"
			);
			if (selected)
				expandAncestors(
					selected,
					document.getElementById("targetFolder")
				);
			return;
		}

		folders.forEach(f => {
			if (f.classList.contains("virtual-letter")) return;
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
					ws.send(JSON.stringify({type: "subscribe", path: ""}));
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
						log("folder added event", {rawPath, newPath});

						target = newPath;
						const url = new URL(location.href);
						url.searchParams.set("target", target);
						window.history.replaceState({}, "", url.toString());

						loadFolders().catch(err =>
							warn("loadFolders after WS failed", err)
						);
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
	const targetParam = normalizePath(query.get("target"));
	if (targetParam) {
		target = targetParam;
	}

	await loadFolders();
	
	if (targetParam) {
		autoExpandTarget(targetParam);
	}
	
	setupFolderSearch();
	setupTargetSearch();
	setupWebsocket();

	const leftContainer = document.getElementById("folderList");
	if (leftContainer)
		leftContainer.addEventListener("click", e =>
			handleFolderClick(e, true)
		);

	const rightContainer = document.getElementById("targetFolder");
	if (rightContainer)
		rightContainer.addEventListener("click", e =>
			handleFolderClick(e, false)
		);

	setupSwipeHandlers();

	if (dir) await loadMedia();
}

function autoExpandTarget(targetPath) {
	const parts = targetPath.split("/").filter(Boolean);
	const rightContainer = document.getElementById("targetFolder");
	
	for (let i = parts.length; i > 0; i--) {
		const pathToFind = parts.slice(0, i).join("/");
		const folders = rightContainer.querySelectorAll(".folder[data-full]");
		
		for (const folder of folders) {
			if (normalizePath(folder.dataset.full) === normalizePath(pathToFind)) {
				folder.classList.add("selected");
				expandAncestors(folder.parentElement, rightContainer);
				setTimeout(() => folder.scrollIntoView({block: "center"}), 50);
				return;
			}
		}
	}
}

init();
