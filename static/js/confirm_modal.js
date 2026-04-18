// Shared confirmation modal handler for in-app POST forms.
// Usage:
// - Include `_confirm_modal.html` somewhere in the page (it is already included in the main bases).
// - Add `data-confirm="Custom message"` for better copy (optional).
// - Add `data-confirm-variant="primary"` (green OK) or `data-confirm-variant="danger"` (red OK). Default: primary.
// - Add `data-confirm-bypass="1"` to opt out.

(function () {
  const modal = document.getElementById("confirm-modal");
  const msg = document.getElementById("confirm-modal-message");
  const ok = modal ? modal.querySelector("[data-confirm-ok]") : null;
  const cancel = modal ? modal.querySelector("[data-confirm-cancel]") : null;

  if (!modal || !msg || !ok || !cancel) return;

  const OK_PRIMARY_CLASSES =
    "rounded-xl bg-emerald-500 px-4 py-2 text-sm font-semibold text-white hover:bg-emerald-600";
  const OK_DANGER_CLASSES =
    "rounded-xl bg-red-500 px-4 py-2 text-sm font-semibold text-white hover:bg-red-600";

  let pendingForm = null;
  const fallbackMessage = "Are you sure you want to continue?";

  function applyOkButtonVariant(formEl) {
    if (!formEl) {
      ok.setAttribute("class", OK_PRIMARY_CLASSES);
      return;
    }
    const v = (formEl.getAttribute("data-confirm-variant") || "primary").trim().toLowerCase();
    ok.setAttribute("class", v === "danger" ? OK_DANGER_CLASSES : OK_PRIMARY_CLASSES);
  }

  function openConfirm(message, formEl) {
    msg.textContent = (message || "").trim() || fallbackMessage;
    pendingForm = formEl || null;
    applyOkButtonVariant(formEl);
    modal.classList.remove("hidden");
    modal.setAttribute("aria-hidden", "false");
  }

  function closeConfirm() {
    modal.classList.add("hidden");
    modal.setAttribute("aria-hidden", "true");
    pendingForm = null;
    applyOkButtonVariant(null);
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
