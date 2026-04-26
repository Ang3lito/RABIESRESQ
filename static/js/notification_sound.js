(function () {
  function play(url) {
    try {
      var a = new Audio(url);
      a.volume = 0.35;
      var p = a.play();
      if (p && typeof p.catch === "function") {
        p.catch(function () {});
      }
    } catch (e) {}
  }

  var root = document.documentElement;
  var role = root.getAttribute("data-notification-sound-role") || "";
  var should = root.getAttribute("data-play-notification-sound") === "1";
  if (!should || !role) return;

  var base = root.getAttribute("data-sound-base") || "/static/sounds/";
  if (base.slice(-1) !== "/") base += "/";
  var file = { patient: "patient.wav", clinic: "clinic.wav", admin: "admin.wav" }[role];
  if (!file) return;
  // Automatic play on page load is disabled per user request to avoid repetitive ping sounds.
  // play(base + file);
})();
