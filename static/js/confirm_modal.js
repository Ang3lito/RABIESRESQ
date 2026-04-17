// Shared confirmation modal handler for in-app POST forms.
// Usage:
// - Include `_confirm_modal.html` somewhere in the page (it is already included in the main bases).
// - Add `data-confirm="Custom message"` for better copy (optional).
// - Add `data-confirm-bypass="1"` to opt out.

(function () {
  const modal = document.getElementById("confirm-modal");
  const msg = document.getElementById("confirm-modal-message");
  const ok = modal ? modal.querySelector("[data-confirm-ok]") : null;
  const cancel = modal ? modal.querySelector("[data-confirm-cancel]") : null;

  if (!modal || !msg || !ok || !cancel) return;

  let pendingForm = null;
  const fallbackMessage = "Are you sure you want to continue?";

  function openConfirm(message, formEl) {
    msg.textContent = (message || "").trim() || fallbackMessage;
    pendingForm = formEl || null;
    modal.classList.remove("hidden");
    modal.setAttribute("aria-hidden", "false");
  }

  function closeConfirm() {
    modal.classList.add("hidden");
    modal.setAttribute("aria-hidden", "true");
    pendingForm = null;
  }

  modal.addEventListener("click", function (e) {
    const t = e.target;
    if (t && (t.matches("[data-confirm-close]") || t.closest("[data-confirm-close]"))) closeConfirm();
  });
  cancel.addEventListener("click", closeConfirm);
  ok.addEventListener("click", function () {
    if (pendingForm) pendingForm.submit();
    closeConfirm();
  });

  document.addEventListener("keydown", function (e) {
    if (e.key === "Escape") closeConfirm();
  });

  document.addEventListener(
    "submit",
    function (e) {
      const form = e.target;
      if (!form || !(form instanceof HTMLFormElement)) return;

      if ((form.getAttribute("data-confirm-bypass") || "").trim() === "1") return;

      const method = (form.getAttribute("method") || "get").trim().toLowerCase();
      if (method !== "post") return;

      e.preventDefault();
      openConfirm(form.getAttribute("data-confirm") || "", form);
    },
    true
  );
})();

