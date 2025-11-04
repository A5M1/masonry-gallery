(function () {
	function el(id) {
		return document.getElementById(id);
	}
	function fetchList() {
		var dir = el("dir") ? el("dir").value : "";
		var url = "/api/thumbdb/list";
		if (dir && dir.length > 0) url += "?dir=" + encodeURIComponent(dir);
		fetch(url)
			.then(r => r.json())
			.then(data => {
				renderList(data.items || []);
			})
			.catch(e => {
				console.error(e);
				alert("Failed to load list");
			});
	}
	function renderList(items) {
		var filter = el("filter").value.toLowerCase();
		var out = [
			"<table><thead><tr><th>Key</th><th>Sizes</th><th>Path (preview)</th><th>Actions</th></tr></thead><tbody>"
		];
		for (var i = 0; i < items.length; i++) {
			var it = items[i];
			if (filter && it.key.toLowerCase().indexOf(filter) === -1) continue;
			var sizes = "";
			if (it.small || it.large) sizes = (it.small || "") + ":" + (it.large || "");
			var v = it.value || "";
			var preview = v.length > 120 ? v.substr(0, 120) + "..." : v;
			out.push(
				"<tr><td>" +
					escapeHtml(it.key) +
					"</td><td>" +
					escapeHtml(sizes) +
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
				el("editor").style.display = "block";
				el("editorKey").textContent = obj.key;
				el("editorValue").value = obj.value || "";
			})
			.catch(e => {
				alert("Failed to load entry");
			});
	}
	function saveEntry() {
		var key = el("editorKey").textContent;
		var val = el("editorValue").value;
		var dir = el("dir") ? el("dir").value : "";
		var url = "/api/thumbdb/set";
		if (dir && dir.length > 0) url += "?dir=" + encodeURIComponent(dir);
		fetch(url, {
			method: "POST",
			headers: {"Content-Type": "application/json"},
			body: JSON.stringify({key: key, value: val})
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
