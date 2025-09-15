(function() {
    const VIDEO_VOLUME = 0.3;
    const urlParams = new URLSearchParams(window.location.search);
    const dir = urlParams.get("dir") || "";
    let page = parseInt(urlParams.get("page")) || 1;
    let hasMore = true;
    let isLoading = false;
    let observer = null;
    const $gallery = $("#gallery");
    const $loading = $("#loading");
    let masonryInstance, debounceTimer;

    function initializeMasonry() {
        masonryInstance = new Masonry($gallery[0], {
            itemSelector: ".masonry-item",
            percentPosition: true,
            columnWidth: ".grid-sizer",
            gutter: 0
        });
    }

    function debounceLayout() {
        clearTimeout(debounceTimer);
        debounceTimer = setTimeout(() => masonryInstance && masonryInstance.layout(), 100);
    }

    function initInfiniteScroll() {
        const loadingEl = document.getElementById("loading");
        if (loadingEl) {
            observer?.disconnect();
            observer = new IntersectionObserver((entries) => {
                if (entries[0].isIntersecting && hasMore && !isLoading) {
                    loadMedia();
                }
            }, {
                rootMargin: "600px"
            });
            observer.observe(loadingEl);
        }
    }

    function lazyLoadImages() {
        const io = new IntersectionObserver((entries, obs) => {
            for (const entry of entries) {
                if (entry.isIntersecting) {
                    const img = entry.target;
                    img.src = img.dataset.src;
                    img.onload = debounceLayout;
                    obs.unobserve(img);
                }
            }
        }, {
            rootMargin: "800px"
        });

        document.querySelectorAll("img.lazy:not([data-observed])").forEach((el) => {
            io.observe(el);
            el.dataset.observed = "true";
        });
    }

    function lazyLoadVideos() {
        const io = new IntersectionObserver((entries, obs) => {
            for (const entry of entries) {
                if (entry.isIntersecting) {
                    const video = entry.target;
                    const source = video.querySelector("source");
                    source.src = source.dataset.src;
                    video.load();
                    video.volume = VIDEO_VOLUME;
                    video.addEventListener("loadedmetadata", debounceLayout, { once: true });
                    obs.unobserve(video);
                }
            }
        }, {
            rootMargin: "500px"
        });

        document.querySelectorAll("video.lazy-video:not([data-observed])").forEach((el) => {
            io.observe(el);
            el.dataset.observed = "true";
        });
    }

    function loadMedia() {
        if (!hasMore || isLoading) return;

        isLoading = true;
        $loading.text("Loading...").show();

        const url = new URL(window.location);
        url.searchParams.set("page", page);
        window.history.pushState({ path: url.href }, "", url.href);

        $.getJSON(`/api/media?dir=${dir}&page=${page}`, (data) => {
            const existing = new Set($gallery.find('a[data-fancybox="gallery"]').map((_, el) => el.getAttribute("href")).get());
            const newItems = data.items.filter(item => !existing.has("/images/" + item.path));

            if (newItems.length) {
                const fragment = document.createDocumentFragment();
                const elements = newItems.map((item) => {
                    const isImage = item.type === "image";
                    const src = "/images/" + item.path;
                    const wrapper = document.createElement("div");
                    wrapper.className = "masonry-item";

                    wrapper.innerHTML = `
                        <a data-fancybox="gallery" href="${src}" ${isImage ? "" : 'data-type="video"'}>
                            ${isImage
                                ? `<img data-src="${src}" class="lazy">`
                                : `<video class="lazy-video" preload="metadata" muted loop>
                                    <source data-src="${src}" type="video/mp4">
                                  </video>`}
                        </a>`;
                    return wrapper;
                });

                elements.forEach(el => fragment.appendChild(el));
                if (!masonryInstance) initializeMasonry();
                $gallery.append(fragment);
                masonryInstance.appended(elements);

                lazyLoadImages();
                lazyLoadVideos();
            }

            hasMore = data.hasMore;
            page++;
            $loading.text(hasMore ? "Loading..." : "No more media");
        }).always(() => {
            isLoading = false;
            if (!hasMore) $loading.hide();
        });
    }

    window.scrollToTopAndReset = function() {
        window.scrollTo({ top: 0, behavior: "smooth" });
        masonryInstance?.destroy();
        $gallery.empty().append('<div class="grid-sizer"></div>');
        initializeMasonry();
        page = 1;
        hasMore = true;
        isLoading = false;

        const url = new URL(window.location.href);
        url.searchParams.delete("page");
        window.history.pushState({ path: url.href }, "", url.href);

        initInfiniteScroll();
        loadMedia();
    };

    $(document).ready(() => {
        $('[data-fancybox="gallery"]').fancybox({
            buttons: ["close"],
            loop: true,
            video: {
                autoplay: false,
                preload: "auto",
                controls: false, // default off
                loop: true
            },
afterShow: (instance, current) => {
    masonryInstance.layout();
    const video = current.$content.find("video").get(0);
    if (video) {
        Object.assign(video, {
            controls: true,
            preload: "auto",   // ✅ allow scrubbing
            volume: VIDEO_VOLUME,
            muted: false,
            loop: true
        });
        video.load(); // force reload with new preload setting
    }
},
            afterClose: (instance, current) => {
                const video = current.$content.find("video").get(0);
                if (video) {
                    video.pause();
                    video.currentTime = 0;
                }
            }
        });

        initializeMasonry();
        initInfiniteScroll();
        loadMedia();
    });

    window.u = loadMedia;
})();
