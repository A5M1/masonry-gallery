(function () {
	function el(id) {
		return document.getElementById(id);
	}
	function normalizeItems(items) {
		if (!items) return [];
		if (Array.isArray(items)) return items.map(normalizeEntry).filter(Boolean);
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
			value = typeof raw.value === "string" ? raw.value : String(raw.value);
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
			"<table><thead><tr><th>Key</th><th>Thumb tokens</th><th>Path (preview)</th><th>Actions</th></tr></thead><tbody>"
		];
		for (var i = 0; i < items.length; i++) {
			var it = items[i];
			if (!it || !it.key) continue;
			var keyLower = it.key.toLowerCase();
			if (filter && keyLower.indexOf(filter) === -1) continue;
			var small = it.small && it.small !== "null" ? it.small : "";
			var large = it.large && it.large !== "null" ? it.large : "";
			var info = "Small: " + (small || "missing") + " • Large: " + (large || "missing");
			var value = it.value || "";
			var preview = value.length > 120 ? value.substr(0, 120) + "..." : value;
			out.push(
				"<tr><td>" +
					escapeHtml(it.key) +
					"</td><td>" +
					escapeHtml(info) +
					'</td><td class="previewCell"><pre style="white-space:pre-wrap;">' +
					escapeHtml(preview) +
					'</pre></td><td><button data-key="' +
					escapeAttr(it.key) +
					'" class="view">View</button></td></tr>'
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
				var smallField = el("editorSmall");
				var largeField = el("editorLarge");
				el("editor").style.display = "block";
				el("editorKey").textContent = obj.key;
				el("editorValue").value = obj.value || "";
				if (smallField) smallField.value = obj.small && obj.small !== "null" ? obj.small : "";
				if (largeField) largeField.value = obj.large && obj.large !== "null" ? obj.large : "";
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
