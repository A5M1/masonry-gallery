(function () {
	function el(id) {
		return document.getElementById(id);
	}
	function normalizeItems(items) {
		if (!items) return [];
		if (Array.isArray(items))
			return items.map(normalizeEntry).filter(Boolean);
		return Object.keys(items)
			.map(function (key) {
				var entry = items[key];
				if (!entry || typeof entry !== "object") entry = {value: entry};
				var copy = Object.assign({}, entry, {key: key});
				return normalizeEntry(copy);
			})
			.filter(Boolean);
	}
	function normalizeEntry(raw) {
		if (!raw) return null;
		var key = raw.key || raw.id || raw.name;
		if (!key) return null;
		var small = typeof raw.small === "string" ? raw.small : "";
		var large = typeof raw.large === "string" ? raw.large : "";
		var value = "";
		if (raw.value !== undefined && raw.value !== null) {
			value =
				typeof raw.value === "string" ? raw.value : String(raw.value);
		}
		return {key: key, small: small, large: large, value: value};
	}
	function fetchList() {
		var dir = el("dir") ? el("dir").value : "";
		var url = "/api/thumbdb/list";
		if (dir && dir.length > 0) url += "?dir=" + encodeURIComponent(dir);
		fetch(url)
			.then(r => r.json())
			.then(data => {
				renderList(normalizeItems(data && data.items));
			})
			.catch(e => {
				console.error(e);
				alert("Failed to load list");
			});
	}
	function renderList(items) {
		var filter = el("filter").value.toLowerCase();
		var out = [
			"<table><thead><tr><th>Key</th><th>Type</th><th>Hash</th><th>Dimensions</th><th>Timestamp</th><th>Path</th><th>Actions</th></tr></thead><tbody>"
		];
		for (var i = 0; i < items.length; i++) {
			var it = items[i];
			if (!it || !it.key) continue;
			var keyLower = it.key.toLowerCase();
			if (filter && keyLower.indexOf(filter) === -1) continue;
			var value = it.value || "";
			var preview = value.length > 60 ? value.substr(0, 60) + "..." : value;
			var detail = it.detail || {};
			var type = detail.media_type || "?";
			var animated = detail.animated ? "🎬" : "";
			var hash = detail.hash ? detail.hash.substr(0, 8) + "..." : "none";
			var dims = "";
			if (detail.width && detail.height) {
				dims = detail.width + "x" + detail.height;
			}
			if (detail.duration) {
				dims += " (" + detail.duration + "s)";
			}
			var ts = detail.timestamp ? new Date(detail.timestamp * 1000).toISOString().substr(0, 19).replace("T", " ") : "";
			out.push(
				"<tr><td>" +
					escapeHtml(it.key) +
					"</td><td>" +
					escapeHtml(type + animated) +
					"</td><td><code style='font-size:9px'>" +
					escapeHtml(hash) +
					"</code></td><td>" +
					escapeHtml(dims) +
					"</td><td style='font-size:10px'>" +
					escapeHtml(ts) +
					"</td><td style='font-size:11px'>" +
					escapeHtml(preview) +
					'</td><td><button data-key="' +
					escapeAttr(it.key) +
					'" class="view">Detail</button></td></tr>'
			);
		}
		out.push("</tbody></table>");
		el("list").innerHTML = out.join("");
		var buttons = document.querySelectorAll("button.view");
		for (var i = 0; i < buttons.length; i++) {
			buttons[i].addEventListener("click", function (ev) {
				openEditor(this.getAttribute("data-key"));
			});
		}
	}

	function openEditor(key) {
		var dir = el("dir") ? el("dir").value : "";
		var url = "/api/thumbdb/get?key=" + encodeURIComponent(key);
		if (dir && dir.length > 0) url += "&dir=" + encodeURIComponent(dir);
		fetch(url)
			.then(r => {
				if (!r.ok) throw new Error("not found");
				return r.json();
			})
			.then(obj => {
				el("editor").style.display = "block";
				el("editorKey").textContent = obj.key;
				el("editorValue").value = obj.value || "";
				var detail = obj.detail || {};
				var detailHtml = "<h3>Binary Format Fields (MediaVault v2.1)</h3>";
				detailHtml += "<table style='width:100%;border-collapse:collapse'>";
				detailHtml += "<tr><th style='text-align:left;padding:4px;border:1px solid #ccc'>Field</th><th style='text-align:left;padding:4px;border:1px solid #ccc'>Value</th></tr>";
				detailHtml += "<tr><td style='padding:4px;border:1px solid #ccc'>Record Sequence</td><td style='padding:4px;border:1px solid #ccc'>" + (detail.record_seq || 0) + "</td></tr>";
				detailHtml += "<tr><td style='padding:4px;border:1px solid #ccc'>Filename</td><td style='padding:4px;border:1px solid #ccc'>" + escapeHtml(obj.key) + "</td></tr>";
				var ts = detail.timestamp ? new Date(detail.timestamp * 1000).toISOString() : "N/A";
				detailHtml += "<tr><td style='padding:4px;border:1px solid #ccc'>Timestamp</td><td style='padding:4px;border:1px solid #ccc'>" + ts + " (" + (detail.timestamp || 0) + ")</td></tr>";
				detailHtml += "<tr><td style='padding:4px;border:1px solid #ccc'>Media Type</td><td style='padding:4px;border:1px solid #ccc'>" + (detail.media_type || "unknown") + "</td></tr>";
				detailHtml += "<tr><td style='padding:4px;border:1px solid #ccc'>Animated</td><td style='padding:4px;border:1px solid #ccc'>" + (detail.animated ? "Yes" : "No") + "</td></tr>";
				detailHtml += "<tr><td style='padding:4px;border:1px solid #ccc'>Thumb Mode</td><td style='padding:4px;border:1px solid #ccc'>" + (detail.thumb_mode || 0) + "</td></tr>";
				detailHtml += "<tr><td style='padding:4px;border:1px solid #ccc'>Hash Mode</td><td style='padding:4px;border:1px solid #ccc'>" + (detail.hash_mode || "unknown") + "</td></tr>";
				detailHtml += "<tr><td style='padding:4px;border:1px solid #ccc'>Hash</td><td style='padding:4px;border:1px solid #ccc'><code style='font-size:11px'>" + (detail.hash || "none") + "</code></td></tr>";
				detailHtml += "<tr><td style='padding:4px;border:1px solid #ccc'>Directory Count</td><td style='padding:4px;border:1px solid #ccc'>" + (detail.dir_count || 0) + "</td></tr>";
				detailHtml += "<tr><td style='padding:4px;border:1px solid #ccc'>Width</td><td style='padding:4px;border:1px solid #ccc'>" + (detail.width || 0) + "px</td></tr>";
				detailHtml += "<tr><td style='padding:4px;border:1px solid #ccc'>Height</td><td style='padding:4px;border:1px solid #ccc'>" + (detail.height || 0) + "px</td></tr>";
				detailHtml += "<tr><td style='padding:4px;border:1px solid #ccc'>Duration</td><td style='padding:4px;border:1px solid #ccc'>" + (detail.duration || 0) + "s</td></tr>";
				detailHtml += "<tr><td style='padding:4px;border:1px solid #ccc'>CRC32</td><td style='padding:4px;border:1px solid #ccc'>" + (detail.crc32 || 0) + "</td></tr>";
				detailHtml += "<tr><td style='padding:4px;border:1px solid #ccc'>Orientation</td><td style='padding:4px;border:1px solid #ccc'>" + (detail.orientation || 0) + "</td></tr>";
				if (detail.gps_lat || detail.gps_lon) {
					detailHtml += "<tr><td style='padding:4px;border:1px solid #ccc'>GPS Coords</td><td style='padding:4px;border:1px solid #ccc'>" + (detail.gps_lat || 0).toFixed(6) + ", " + (detail.gps_lon || 0).toFixed(6) + "</td></tr>";
				}
				detailHtml += "</table>";
				var detailDiv = el("editorDetail");
				if (detailDiv) detailDiv.innerHTML = detailHtml;
			})
			.catch(e => {
				alert("Failed to load entry");
			});
	}
	function saveEntry() {
		var key = el("editorKey").textContent;
		var smallField = el("editorSmall");
		var largeField = el("editorLarge");
		var smallToken = smallField ? smallField.value.trim() : "";
		var largeToken = largeField ? largeField.value.trim() : "";
		if (!smallToken) smallToken = "null";
		if (!largeToken) largeToken = "null";
		var runtimeValue = el("editorValue").value;
		var payloadValue = smallToken + ";" + largeToken + ";" + runtimeValue;
		var dir = el("dir") ? el("dir").value : "";
		var url = "/api/thumbdb/set";
		if (dir && dir.length > 0) url += "?dir=" + encodeURIComponent(dir);
		fetch(url, {
			method: "POST",
			headers: {"Content-Type": "application/json"},
			body: JSON.stringify({key: key, value: payloadValue})
		})
			.then(r => {
				if (!r.ok) throw new Error("save failed");
				return r.text();
			})
			.then(() => {
				el("editor").style.display = "none";
				fetchList();
			})
			.catch(e => {
				alert("Save failed");
			});
	}
	function deleteEntry() {
		if (!confirm("Delete this entry?")) return;
		var key = el("editorKey").textContent;
		var dir = el("dir") ? el("dir").value : "";
		var url = "/api/thumbdb/delete";
		if (dir && dir.length > 0) url += "?dir=" + encodeURIComponent(dir);
		fetch(url, {
			method: "POST",
			headers: {"Content-Type": "application/json"},
			body: JSON.stringify({key: key})
		})
			.then(r => {
				if (!r.ok) throw new Error("delete failed");
				return r.text();
			})
			.then(() => {
				el("editor").style.display = "none";
				fetchList();
			})
			.catch(e => {
				alert("Delete failed");
			});
	}
	function escapeHtml(s) {
		return s.replace(/[&<>]/g, function (c) {
			return {"&": "&amp;", "<": "&lt;", ">": "&gt;"}[c];
		});
	}
	function escapeAttr(s) {
		return s.replace(/"/g, "&quot;");
	}
	function viewThumbsForDir() {
		var dir = el("dir") ? el("dir").value : "";
		if (!dir || dir.length === 0) {
			alert("Enter a gallery dir");
			return;
		}
		fetch("/api/thumbdb/thumbs_for_dir?dir=" + encodeURIComponent(dir))
			.then(r => {
				if (!r.ok) throw new Error("failed");
				return r.json();
			})
			.then(obj => {
				if (obj.url) window.open(obj.url, "_blank");
				else alert("No thumbs url");
			})
			.catch(e => {
				alert("Failed to open thumbs for dir");
			});
	}
	document.addEventListener("DOMContentLoaded", function () {
		el("refresh").addEventListener("click", fetchList);
		el("clear").addEventListener("click", function () {
			el("filter").value = "";
			fetchList();
		});
		el("save").addEventListener("click", saveEntry);
		el("del").addEventListener("click", deleteEntry);
		el("close").addEventListener("click", function () {
			el("editor").style.display = "none";
		});
		if (el("viewthumbs"))
			el("viewthumbs").addEventListener("click", viewThumbsForDir);
		fetchList();
	});
})();
