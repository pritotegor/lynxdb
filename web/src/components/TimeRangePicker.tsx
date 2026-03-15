import { useRef, useEffect, useCallback, useState } from "preact/hooks";
import { signal } from "@preact/signals";
import type { Signal } from "@preact/signals";
import {
  PRESETS,
  getTimeRangeLabel,
  parseRelativeExpression,
} from "../utils/timeFormat";
import styles from "./TimeRangePicker.module.css";

interface TimeRangePickerProps {
  from: Signal<string>;
  to: Signal<string | undefined>;
  onApply?: () => void;
}

const open = signal(false);

export function TimeRangePicker({ from, to, onApply }: TimeRangePickerProps) {
  const wrapperRef = useRef<HTMLDivElement>(null);
  const [customFrom, setCustomFrom] = useState("");
  const [customTo, setCustomTo] = useState("");
  const [relativeExpr, setRelativeExpr] = useState("");
  const [validationError, setValidationError] = useState<string | null>(null);

  const handleSelect = useCallback(
    (value: string) => {
      from.value = value;
      to.value = undefined;
      open.value = false;
      onApply?.();
    },
    [from, to, onApply],
  );

  const handleApply = useCallback(() => {
    setValidationError(null);

    // Priority: relative expression > datetime-local inputs
    if (relativeExpr.trim()) {
      const parsed = parseRelativeExpression(relativeExpr);
      if (!parsed) {
        setValidationError("Invalid format. Use: 2h ago to 30m ago");
        return;
      }
      from.value = parsed.from;
      to.value = parsed.to;
      open.value = false;
      setRelativeExpr("");
      setCustomFrom("");
      setCustomTo("");
      onApply?.();
      return;
    }

    if (customFrom && customTo) {
      from.value = new Date(customFrom).toISOString();
      to.value = new Date(customTo).toISOString();
      open.value = false;
      setCustomFrom("");
      setCustomTo("");
      setRelativeExpr("");
      onApply?.();
      return;
    }

    // Neither input has values -- do nothing
  }, [from, to, onApply, relativeExpr, customFrom, customTo]);

  // Close dropdown on outside click
  useEffect(() => {
    function onPointerDown(e: PointerEvent) {
      if (
        wrapperRef.current &&
        !wrapperRef.current.contains(e.target as Node)
      ) {
        open.value = false;
      }
    }
    document.addEventListener("pointerdown", onPointerDown, true);
    return () =>
      document.removeEventListener("pointerdown", onPointerDown, true);
  }, []);

  return (
    <div class={styles.wrapper} ref={wrapperRef}>
      <button
        type="button"
        class={styles.trigger}
        onClick={() => {
          open.value = !open.value;
        }}
        aria-haspopup="listbox"
        aria-expanded={open.value}
      >
        <span class={styles.triggerIcon}>&#9202;</span>
        {getTimeRangeLabel(from.value, to.value)}
      </button>
      {open.value && (
        <div class={styles.dropdown} role="listbox" aria-label="Time range">
          {PRESETS.map((preset) => (
            <button
              key={preset.value}
              type="button"
              role="option"
              aria-selected={from.value === preset.value}
              class={`${styles.option} ${from.value === preset.value ? styles.optionActive : ""}`}
              onClick={() => handleSelect(preset.value)}
            >
              {preset.label}
            </button>
          ))}

          <div class={styles.divider} />

          <div class={styles.customSection}>
            <div class={styles.customRow}>
              <label class={styles.customLabel}>From</label>
              <input
                type="datetime-local"
                class={styles.customInput}
                value={customFrom}
                onInput={(e) => {
                  setCustomFrom((e.target as HTMLInputElement).value);
                  setValidationError(null);
                }}
              />
            </div>
            <div class={styles.customRow}>
              <label class={styles.customLabel}>To</label>
              <input
                type="datetime-local"
                class={styles.customInput}
                value={customTo}
                onInput={(e) => {
                  setCustomTo((e.target as HTMLInputElement).value);
                  setValidationError(null);
                }}
              />
            </div>

            <div class={styles.customRow}>
              <label class={styles.customLabel}>Relative</label>
              <input
                type="text"
                class={styles.customInput}
                placeholder="e.g. 2h ago to 30m ago"
                value={relativeExpr}
                onInput={(e) => {
                  setRelativeExpr((e.target as HTMLInputElement).value);
                  setValidationError(null);
                }}
                onKeyDown={(e) => {
                  if (e.key === "Enter") {
                    e.preventDefault();
                    handleApply();
                  }
                }}
              />
            </div>

            {validationError && (
              <div class={styles.validationError}>{validationError}</div>
            )}

            <button
              type="button"
              class={styles.applyBtn}
              onClick={handleApply}
            >
              Apply
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
