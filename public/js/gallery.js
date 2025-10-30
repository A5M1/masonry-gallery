
!function () {
    let galleryVolume = 0.02;
    const queryParams = new URLSearchParams(window.location.search);
    const currentDir = queryParams.get("dir") || "";
    let masonryInstance, sentinelObserver, currentPage = 1, hasMore = true, isLoading = false, firstLoad = true, lastItemObserver = null;
    const galleryContainer = $("#gallery");
    const loadingIndicator = $("#loading");

    function setCookie(name, value, days) {
        const expiry = new Date();
        expiry.setTime(expiry.getTime() + days * 24 * 60 * 60 * 1000);
        document.cookie = `${name}=${encodeURIComponent(value)};expires=${expiry.toUTCString()};path=/`;
    }

    function getCookie(name) {
        const match = document.cookie.match("(^|;)\\s*" + name + "=([^;]*)");
        return match ? decodeURIComponent(match[2]) : null;
    }

    const savedVolume = getCookie("gallery_volume");
    if (savedVolume !== null) {
        const parsed = parseFloat(savedVolume);
        if (!Number.isNaN(parsed) && parsed >= 0 && parsed <= 1) galleryVolume = parsed;
    }

    document.addEventListener("volumechange", evt => {
        const el = evt.target;
        if (el && el.tagName === "VIDEO") {
            const vol = el.volume;
            if (!Number.isNaN(vol)) {
                galleryVolume = vol;
                setCookie("gallery_volume", vol, 365);
            }
        }
    }, true);

    function initMasonry() {
        masonryInstance = new Masonry(galleryContainer[0], {
            itemSelector: ".masonry-item",
            percentPosition: true,
            columnWidth: ".grid-sizer",
            gutter: 0,
            transitionDuration: 0
        });
    }

    let layoutQueued = false;
    function scheduleLayout() {
        if (!layoutQueued) {
            layoutQueued = true;
            requestAnimationFrame(() => {
                if (masonryInstance) masonryInstance.layout();
                layoutQueued = false;
            });
        }
    }

    function observeLazyElements(selector, handler, margin) {
        const observer = new IntersectionObserver((entries, obs) => {
            for (const entry of entries) {
                if (entry.isIntersecting) {
                    try {
                        if (handler(entry.target)) obs.unobserve(entry.target);
                    } catch (err) {
                        console.error("Lazy load handler failed", err);
                        obs.unobserve(entry.target);
                    }
                }
            }
        }, { rootMargin: margin });
        document.querySelectorAll(`${selector}:not([data-observed])`).forEach(el => {
            observer.observe(el);
            el.dataset.observed = "true";
        });
    }

    function initInfiniteScroll() {
        const loadingEl = document.getElementById("loading");
        if (!loadingEl) return;
        if (sentinelObserver) sentinelObserver.disconnect();
        sentinelObserver = new IntersectionObserver(entries => {
            const entry = entries && entries[0];
            if (entry && entry.isIntersecting && hasMore && !isLoading) loadNextPage();
        }, { rootMargin: "600px" });
        sentinelObserver.observe(loadingEl);
        observeLastItem();
    }

    function observeLastItem() {
        if (lastItemObserver) {
            try { lastItemObserver.disconnect(); } catch {}
            lastItemObserver = null;
        }
        const items = document.querySelectorAll("#gallery .masonry-item");
        if (!items || items.length === 0) return;
        const lastItem = items[items.length - 1];
        lastItemObserver = new IntersectionObserver(entries => {
            const entry = entries && entries[0];
            if (entry && entry.isIntersecting && hasMore && !isLoading) loadNextPage();
        }, { rootMargin: "400px" });
        lastItemObserver.observe(lastItem);
    }

    function loadNextPage() {
        if (!hasMore || isLoading) return;
        isLoading = true;
        loadingIndicator.text("Loading...").show();
        $.get(`/api/media?dir=${encodeURIComponent(currentDir)}&page=${currentPage}&render=html`, html => {
            try {
                const temp = document.createElement("div");
                temp.innerHTML = html;
                const fragment = temp.querySelector(".masonry-fragment");
                if (!fragment) return console.warn("No masonry fragment returned from server");

                const nextHasMore = fragment.getAttribute("data-hasmore") === "1";
                const newItems = Array.from(fragment.children);
                if (newItems.length) {
                    const frag = document.createDocumentFragment();
                    newItems.forEach(el => frag.appendChild(el));
                    if (!masonryInstance) initMasonry();
                    galleryContainer.append(frag);
                    masonryInstance.appended(newItems);

                    if (typeof imagesLoaded === "function") {
                        imagesLoaded(galleryContainer[0], () => {
                            masonryInstance.layout();
                            observeLastItem();
                        });
                    } else {
                        setTimeout(() => {
                            masonryInstance.layout();
                            observeLastItem();
                        }, 100);
                    }

                    observeLazyElements("img.thumb-img", el => {
                        const small = el.getAttribute("data-thumb-small");
                        const large = el.getAttribute("data-thumb-large");
                        if (!small) return false;
                        const src = el.getAttribute("src") || "";
                        if (src.includes("base64") || src.includes("placeholder") || src === "") el.src = small;
                        const onLoad = () => {
                            scheduleLayout();
                            if (!large) return el.removeEventListener("load", onLoad);
                            const img = new Image();
                            img.onload = () => { el.src = large; scheduleLayout(); };
                            img.src = large;
                            el.removeEventListener("load", onLoad);
                        };
                        el.addEventListener("load", onLoad, { once: true });
                        return true;
                    }, "800px");

                    observeLazyElements("video.lazy-video", el => {
                        const source = el.querySelector("source");
                        if (!source) return false;
                        const src = source.dataset.src || source.getAttribute("data-src");
                        if (!src) return false;
                        source.src = src;
                        el.load();
                        el.volume = galleryVolume;
                        el.addEventListener("loadedmetadata", scheduleLayout, { once: true });
                        return true;
                    }, "500px");

                    initFancybox();
                }

                hasMore = nextHasMore;
                if (firstLoad) {
                    initInfiniteScroll();
                    firstLoad = false;
                }
                currentPage++;
                loadingIndicator.text(hasMore ? "Loading..." : "No more media");
            } catch (err) {
                console.error("Error handling server fragment", err);
            }
        }).fail((xhr, status, err) => {
            console.error("Failed to load media", status, err);
        }).always(() => {
            isLoading = false;
            if (!hasMore) loadingIndicator.hide();
        });
    }

    function updateScrollButton() {
        const btn = document.querySelector(".scroll-to-top-btn");
        if (!btn) return;
        if ((currentPage || 1) > 1) btn.classList.remove("hidden");
        else btn.classList.add("hidden");
    }

    function initFancybox() {
        $('[data-fancybox="gallery"]').fancybox({
            buttons: ["close"],
            loop: true,
            video: { autoplay: false, preload: "none", controls: false, loop: true },
            afterShow: (inst, obj) => {
                masonryInstance.layout();
                const video = obj.$content.find("video").get(0);
                if (video) {
                    const srcEl = video.querySelector("source");
                    if (srcEl && !srcEl.src) srcEl.src = srcEl.dataset.src || srcEl.getAttribute("data-src") || srcEl.src;
                    Object.assign(video, { controls: true, preload: "metadata", volume: galleryVolume, muted: false, loop: true });
                    video.load();
                }
            },
            afterClose: (inst, obj) => {
                const video = obj.$content.find("video").get(0);
                if (video) { video.pause(); video.currentTime = 0; }
            }
        });
    }

    window.scrollToTopAndReset = function () {
        window.scrollTo({ top: 0, behavior: "smooth" });
        if (masonryInstance) masonryInstance.destroy();
        galleryContainer.empty().append('<div class="grid-sizer"></div>');
        initMasonry();
        currentPage = 1;
        hasMore = true;
        isLoading = false;
        initInfiniteScroll();
        loadNextPage();
    };

    function loadFolderTree() {
        const dir = queryParams.get("dir") || "";
        function renderNode(node) {
            if (!node || !node.children) return "";
            return (
                '<ul class="folder-list">' +
                node.children.map(child => {
                    const isActive = dir === child.path;
                    const hasChildren = child.children && child.children.length > 0;
                    const isOpen = isActive || dir.startsWith(child.path + "/");
                    return `<li class="folder-item ${hasChildren ? "has-children" : ""} ${isOpen ? "open" : ""}" data-path="${child.path}">
                        ${hasChildren ? '<span class="folder-toggle"></span>' : '<span class="folder-spacer"></span>'}
                        <a href="/?dir=${child.path}" class="folder-link ${isActive ? "active" : ""}">
                            📁<span class="folder-name">${child.name}</span>
                        </a>
                        ${hasChildren ? `<div class="folder-children" style="display:${isOpen ? "block" : "none"};">${renderNode(child)}</div>` : ""}
                    </li>`;
                }).join("") +
                "</ul>"
            );
        }
        document.addEventListener("DOMContentLoaded", () => {
            fetch("/api/tree")
                .then(res => res.ok ? res.json() : Promise.reject(res.status))
                .then(data => {
                    const treeContainer = document.getElementById("folder-tree-container");
                    if (!data) {
                        treeContainer.innerHTML = "<p>No media folders found.</p>";
                        return;
                    }
                    treeContainer.innerHTML = renderNode(data);
                    document.querySelectorAll(".folder-item.has-children").forEach(item => {
                        const toggle = item.querySelector(".folder-toggle");
                        const children = item.querySelector(".folder-children");
                        const toggleFn = () => {
                            item.classList.toggle("open");
                            children.style.display = item.classList.contains("open") ? "block" : "none";
                            scheduleLayout();
                        };
                        toggle.addEventListener("click", e => { e.stopPropagation(); toggleFn(); });
                        item.querySelector(".folder-link")?.addEventListener("click", toggleFn);
                    });
                    const active = document.querySelector(`.folder-item[data-path="${dir}"]`);
                    if (active) {
                        let parent = active;
                        while (parent && parent.classList && parent.classList.contains("folder-item")) {
                            parent.classList.add("open");
                            const sub = parent.querySelector(".folder-children");
                            if (sub) sub.style.display = "block";
                            parent = parent.parentElement ? parent.parentElement.closest(".folder-item") : null;
                        }
                        setTimeout(() => active.scrollIntoView({ block: "center", behavior: "smooth" }), 300);
                    }
                })
                .catch(() => {
                    document.getElementById("folder-tree-container").innerHTML = "<p>Error loading folders.</p>";
                });
        });
    }

    loadFolderTree();

    function setupSidebarLogic() {
        const topBtn = document.querySelector(".scroll-to-top-btn");
        const pushState = history.pushState;
        history.pushState = function (...args) { pushState.apply(this, args); updateScrollButton(); };
        const replaceState = history.replaceState;
        history.replaceState = function (...args) { replaceState.apply(this, args); updateScrollButton(); };
        window.addEventListener("popstate", updateScrollButton);
        document.addEventListener("DOMContentLoaded", () => {
            updateScrollButton();
            if (window.innerWidth > 768 && localStorage.getItem("sidebarState") === "open") {
                document.querySelector(".sidebar")?.classList.add("open");
            }
        });
        let scrollTimer = null;
        window.addEventListener("scroll", () => {
            if (!scrollTimer) {
                scrollTimer = setTimeout(() => {
                    scrollTimer = null;
                    if (topBtn) topBtn.classList.toggle("show", window.scrollY > 200);
                    if (window.innerHeight + window.scrollY >= document.documentElement.scrollHeight - 240) {
                        if (hasMore && !isLoading) loadNextPage();
                    }
                }, 150);
            }
        });
        window.onbeforeunload = () => window.scrollTo(0, 0);
        document.addEventListener("click", e => {
            if (window.innerWidth <= 768 && !e.target.closest(".sidebar") && !e.target.matches(".sidebar-toggle-btn"))
                document.querySelector(".sidebar")?.classList.remove("open");
        });
        document.addEventListener("keydown", e => {
            if (e.key === "Escape") document.querySelector(".sidebar")?.classList.remove("open");
        });
        document.addEventListener("play", evt => {
            if (evt.target.tagName === "VIDEO") Object.assign(evt.target, { volume: galleryVolume, muted: false, loop: true });
        }, true);
    }

    setupSidebarLogic();

    window.w = initFancybox;
    window.m = initMasonry;
    window.f = initInfiniteScroll;
    window.v = loadNextPage;
    window.u = loadNextPage;
}();
