/**
 * Password visibility toggle (shared).
 *
 * Usage:
 * - Input must have an id.
 * - Button: type="button" data-toggle-password="<input-id>"
 * - Inside the button, include two SVGs:
 *   - .eye-open (hidden by default)
 *   - .eye-closed (visible by default)
 */
(function () {
  function setButtonState(btn, isVisible) {
    if (!btn) return;
    btn.setAttribute("aria-pressed", isVisible ? "true" : "false");
    btn.setAttribute("aria-label", isVisible ? "Hide password" : "Show password");

    var open = btn.querySelector(".eye-open");
    var closed = btn.querySelector(".eye-closed");
    if (open) open.classList.toggle("hidden", !isVisible);
    if (closed) closed.classList.toggle("hidden", isVisible);
  }

  function toggleForButton(btn) {
    if (!btn) return;
    var id = btn.getAttribute("data-toggle-password");
    if (!id) return;
    var input = document.getElementById(id);
    if (!input) return;

    var isPassword = input.type === "password";
    input.type = isPassword ? "text" : "password";
    setButtonState(btn, isPassword);
  }

  document.addEventListener("click", function (e) {
    var btn = e.target && e.target.closest ? e.target.closest("[data-toggle-password]") : null;
    if (!btn) return;
    e.preventDefault();
    toggleForButton(btn);
  });

  // Initialize aria + icon state for any toggles present on first render.
  document.addEventListener("DOMContentLoaded", function () {
    document.querySelectorAll("[data-toggle-password]").forEach(function (btn) {
      var id = btn.getAttribute("data-toggle-password");
      var input = id ? document.getElementById(id) : null;
      var isVisible = !!(input && input.type === "text");
      setButtonState(btn, isVisible);
    });
  });
})();

