// This file contains the logic for the sidebar folder tree.
(function() {
    const a = new URLSearchParams(window.location.search).get("dir") || "";

    function buildTree(e) {
        if (!e?.children) {
            return "";
        }
        let t = '<ul class="folder-list">';
        e.children.forEach((e) => {
            const o = a === e.path;
            const n = e.children?.length > 0;
            const s = o || a.startsWith(e.path + "/");
            t += `<li class="folder-item ${n ? "has-children" : ""} ${s ? "open" : ""}" data-path="${e.path}" data-name="${e.name.toLowerCase()}">`;
            t += n ? '<span class="folder-toggle"></span>' : '<span class="folder-spacer"></span>';
            t += `<a href="/?dir=${e.path}" class="folder-link ${o ? "active" : ""}"> 📁 <span class="folder-name">${e.name}</span> </a>`;
            if (n) {
                t += `<div class="folder-children" style="display: ${s ? "block" : "none"};">${buildTree(e)}</div>`;
            }
            t += "</li>";
        });
        t += "</ul>";
        return t;
    }

    function initFolderToggles() {
        document.querySelectorAll(".folder-item.has-children").forEach((e) => {
            const t = e.querySelector(".folder-toggle");
            const o = e.querySelector(".folder-children");
            const toggleOpen = () => {
                e.classList.toggle("open");
                o.style.display = e.classList.contains("open") ? "block" : "none";
                debounceLayout();
            };
            t.addEventListener("click", (e) => {
                e.stopPropagation();
                toggleOpen();
            });
            e.querySelector(".folder-link")?.addEventListener("click", (e) => {
                if (e.target.classList.contains("folder-name") || e.target.classList.contains("folder-link")) {
                    toggleOpen();
                }
            });
        });
    }

    document.addEventListener("DOMContentLoaded", () => {
        fetch("/api/tree")
            .then((e) => {
                if (!e.ok) {
                    throw new Error(`HTTP error! status: ${e.status}`);
                }
                return e.json();
            })
            .then((e) => {
                const t = document.getElementById("folder-tree-container");
                if (e) {
                    t.innerHTML = buildTree(e);
                    initFolderToggles();
                    const o = document.querySelector(`.folder-item[data-path="${a}"]`);
                    if (o) {
                        let e = o;
                        while (e?.classList.contains("folder-item")) {
                            e.classList.add("open");
                            const t = e.querySelector(".folder-children");
                            if (t) {
                                t.style.display = "block";
                            }
                            e = e.parentElement.closest(".folder-item");
                        }
                        setTimeout(() => o.scrollIntoView({
                            block: "center",
                            behavior: "smooth"
                        }), 300);
                    }
                } else {
                    t.innerHTML = "<p>No media folders found.</p>";
                }
            })
            .catch(() => {
                document.getElementById("folder-tree-container").innerHTML = "<p>Error loading folders.</p>";
            });
    });
})();