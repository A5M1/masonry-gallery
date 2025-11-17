import $ from "jquery";
window.$ = $;
window.jQuery = $;
import EvEmitter from "ev-emitter";
import imagesLoaded from "imagesloaded";
(async function() {
  try {
    await import("../vendor/jquery.fancybox.min.js");
  } catch (e) {
  }
})();
import Masonry from "masonry-layout";
$.fn.masonry = function (options) {
  return this.each(function () {
    if (!this._masonryInstance) {
      this._masonryInstance = new Masonry(this, options);
    }
  });
};
import Isotope from "isotope-layout";
$.fn.isotope = function (options) {
  return this.each(function () {
    if (!this._isotopeInstance) {
      this._isotopeInstance = new Isotope(this, options);
    }
  });
};
window.EvEmitter = EvEmitter;
window.imagesLoaded = imagesLoaded;
window.Masonry = Masonry;
window.Isotope = Isotope;
window.Fancybox = $.fancybox;
export { $, EvEmitter, imagesLoaded, Masonry, Isotope };
