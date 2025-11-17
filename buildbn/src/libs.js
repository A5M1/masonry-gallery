import $ from "jquery";
window.$ = $;
window.jQuery = $;

import EvEmitter from "ev-emitter";
import imagesLoaded from "imagesloaded";
import Masonry from "masonry-layout";
import Isotope from "isotope-layout";

(async () => {
  try {
    await import("../vendor/jquery.fancybox.min.js");
    window.Fancybox = $.fancybox;
  } catch (e) {
    console.error("Fancybox failed to load:", e);
  }
})();

$.fn.masonry = function (options) {
  return this.each(function () {
    if (!this._masonryInstance) {
      this._masonryInstance = new Masonry(this, options);
    }
  });
};

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

export { 
  default as $, 
  default as jQuery, 
  EvEmitter, 
  imagesLoaded, 
  Masonry, 
  Isotope 
};
