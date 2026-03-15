/**
 * Shared utility functions.
 */

/** Extract a readable label from a URN (last 3 path segments). */
export function urnLabel(urn) {
  const path = urn.split(":").pop() || urn;
  const parts = path.split("/");
  if (parts.length <= 3) return parts.join("/");
  return parts.slice(-3).join("/");
}

/** Format a metadata value for display. */
export function formatMetadataValue(v) {
  if (v === null || v === undefined) return "";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

/** Create a DOM element with optional classes and text. */
export function el(tag, classNames, text) {
  const elem = document.createElement(tag);
  if (classNames) elem.className = classNames;
  if (text !== undefined) elem.textContent = text;
  return elem;
}
