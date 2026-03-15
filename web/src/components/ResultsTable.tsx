import { useRef, useState, useCallback, useLayoutEffect } from "preact/hooks";
import { ChevronRight } from "lucide-preact";
import type { QueryResult, EventsResult, AggregateResult } from "../api/client";
import { updateSortInQuery, parseSortFromQuery } from "../utils/sortQuery";
import styles from "./ResultsTable.module.css";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface ResultsTableProps {
  result: QueryResult | null;
  onRowClick: (row: Record<string, unknown>) => void;
  selectedRow: Record<string, unknown> | null;
  onSort?: (newQuery: string) => void;
  currentQuery?: string;
  isAggregation?: boolean;
  wrap?: boolean;
  onCellCopy?: (value: string, x: number, y: number) => void;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const ROW_HEIGHT = 28;
const OVERSCAN = 5;
const MIN_COL_WIDTH = 60;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Format an ISO timestamp to HH:mm:ss.SSS */
function formatTime(value: unknown): string {
  if (typeof value !== "string") return String(value ?? "");
  try {
    const d = new Date(value);
    if (isNaN(d.getTime())) return String(value);
    const h = String(d.getHours()).padStart(2, "0");
    const m = String(d.getMinutes()).padStart(2, "0");
    const s = String(d.getSeconds()).padStart(2, "0");
    const ms = String(d.getMilliseconds()).padStart(3, "0");
    return `${h}:${m}:${s}.${ms}`;
  } catch {
    return String(value);
  }
}

/** Truncate a string to maxLen characters */
function truncate(value: unknown, maxLen = 200): string {
  const str = value == null ? "" : String(value);
  return str.length > maxLen ? str.slice(0, maxLen) + "\u2026" : str;
}

/** Check if a value is numeric */
function isNumeric(value: unknown): boolean {
  return typeof value === "number" || (typeof value === "string" && value !== "" && !isNaN(Number(value)));
}

/** Derive columns from events: _time first, then _raw, _source, source, then alphabetical */
function deriveColumnsFromEvents(events: Record<string, unknown>[]): string[] {
  const keySet = new Set<string>();
  const limit = Math.min(events.length, 100);
  for (let i = 0; i < limit; i++) {
    for (const key of Object.keys(events[i])) {
      keySet.add(key);
    }
  }

  const priority = ["_time", "_raw", "_source", "source"];
  const ordered: string[] = [];
  for (const p of priority) {
    if (keySet.has(p)) {
      ordered.push(p);
      keySet.delete(p);
    }
  }

  const rest = Array.from(keySet).sort();
  return ordered.concat(rest);
}

/** Normalize result data into a uniform shape for rendering */
function useTableData(result: QueryResult | null): {
  columns: string[];
  rowCount: number;
  getRow: (index: number) => Record<string, unknown>;
  isAgg: boolean;
} {
  if (!result) {
    return { columns: [], rowCount: 0, getRow: () => ({}), isAgg: false };
  }

  if (result.type === "events") {
    const evts = (result as EventsResult).events;
    const columns = deriveColumnsFromEvents(evts);
    return {
      columns,
      rowCount: evts.length,
      getRow: (i: number) => evts[i] ?? {},
      isAgg: false,
    };
  }

  // aggregate or timechart
  const agg = result as AggregateResult;
  const columns = agg.columns;
  return {
    columns,
    rowCount: agg.rows.length,
    getRow: (i: number) => {
      const row: Record<string, unknown> = {};
      const data = agg.rows[i];
      if (data) {
        for (let c = 0; c < columns.length; c++) {
          row[columns[c]] = data[c];
        }
      }
      return row;
    },
    isAgg: true,
  };
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export function ResultsTable({
  result,
  onRowClick,
  selectedRow,
  onSort,
  currentQuery,
  isAggregation,
  wrap = false,
  onCellCopy,
}: ResultsTableProps) {
  const scrollContainerRef = useRef<HTMLDivElement>(null);
  const [scrollTop, setScrollTop] = useState(0);
  const [viewportHeight, setViewportHeight] = useState(600);
  const [columnWidths, setColumnWidths] = useState<Record<string, number>>({});
  const [resizingCol, setResizingCol] = useState<string | null>(null);

  const { columns, rowCount, getRow, isAgg } = useTableData(result);

  const effectiveIsAgg = isAggregation ?? isAgg;

  // Parse current sort state from query
  const currentSort = currentQuery ? parseSortFromQuery(currentQuery) : null;

  // Track viewport height via ResizeObserver
  useLayoutEffect(() => {
    const el = scrollContainerRef.current;
    if (!el) return;

    const obs = new ResizeObserver((entries) => {
      for (const entry of entries) {
        setViewportHeight(entry.contentRect.height);
      }
    });
    obs.observe(el);
    setViewportHeight(el.clientHeight || 600);

    return () => obs.disconnect();
  }, []);

  const handleScroll = useCallback(() => {
    if (scrollContainerRef.current) {
      setScrollTop(scrollContainerRef.current.scrollTop);
    }
  }, []);

  // ---- Column resize handlers ----

  const handleResizeStart = useCallback(
    (e: PointerEvent, colName: string) => {
      e.preventDefault();
      e.stopPropagation();

      const target = e.currentTarget as HTMLElement;
      target.setPointerCapture(e.pointerId);

      const startX = e.clientX;
      // Get the actual rendered width of the column header cell
      const headerCell = target.parentElement;
      const startWidth = columnWidths[colName] || headerCell?.offsetWidth || 180;

      setResizingCol(colName);

      const onMove = (me: PointerEvent) => {
        const delta = me.clientX - startX;
        const newWidth = Math.max(MIN_COL_WIDTH, startWidth + delta);
        setColumnWidths((prev) => ({ ...prev, [colName]: newWidth }));
      };

      const onUp = () => {
        setResizingCol(null);
        target.removeEventListener("pointermove", onMove as EventListener);
        target.removeEventListener("pointerup", onUp);
      };

      target.addEventListener("pointermove", onMove as EventListener);
      target.addEventListener("pointerup", onUp);
    },
    [columnWidths],
  );

  const handleResizeDblClick = useCallback(
    (e: MouseEvent, colName: string) => {
      e.preventDefault();
      e.stopPropagation();
      // Remove stored width to reset to auto-fit (max-content)
      setColumnWidths((prev) => {
        const next = { ...prev };
        delete next[colName];
        return next;
      });
    },
    [],
  );

  // ---- Sort handler ----

  const handleHeaderClick = useCallback(
    (colName: string) => {
      if (!onSort || !currentQuery) return;

      let newDirection: "asc" | "desc" | null;
      if (!currentSort || currentSort.field !== colName) {
        newDirection = "asc";
      } else if (currentSort.direction === "asc") {
        newDirection = "desc";
      } else {
        newDirection = null;
      }

      const newQuery = updateSortInQuery(currentQuery, colName, newDirection);
      onSort(newQuery);
    },
    [onSort, currentQuery, currentSort],
  );

  // ---- Early returns ----

  if (!result) {
    return <div class={styles.empty}>No results</div>;
  }

  if (rowCount === 0) {
    return <div class={styles.empty}>Query returned no results</div>;
  }

  // ---- Grid template ----

  // Build grid-template-columns: gutter + data columns
  const dataColTemplate = columns
    .map((col) => {
      const stored = columnWidths[col];
      if (stored != null) return `${stored}px`;
      // Default auto-fit widths
      if (col === "_time") return "minmax(80px, max-content)";
      if (col === "_raw" || col === "message") return "minmax(200px, max-content)";
      return "minmax(80px, max-content)";
    })
    .join(" ");

  const gridTemplate = `24px ${dataColTemplate}`;
  const gridStyle = { gridTemplateColumns: gridTemplate };

  // ---- Determine if _time is sticky ----
  const hasTimeCol = !effectiveIsAgg && columns.includes("_time");

  // ---- Virtual scroll calculations ----
  const useVirtualScroll = !wrap;
  const totalHeight = rowCount * ROW_HEIGHT;

  let startIndex: number;
  let endIndex: number;

  if (useVirtualScroll) {
    startIndex = Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - OVERSCAN);
    const visibleCount = Math.ceil(viewportHeight / ROW_HEIGHT) + OVERSCAN * 2;
    endIndex = Math.min(rowCount, startIndex + visibleCount);
  } else {
    startIndex = 0;
    endIndex = rowCount;
  }

  // ---- Render rows ----

  const visibleRows = [];
  for (let i = startIndex; i < endIndex; i++) {
    const row = getRow(i);
    const isSelected =
      selectedRow === row ||
      (selectedRow !== null &&
        selectedRow._time === row._time &&
        selectedRow._raw === row._raw);

    const rowClasses = [
      useVirtualScroll ? styles.row : styles.rowWrap,
      isSelected ? styles.rowSelected : "",
    ]
      .filter(Boolean)
      .join(" ");

    const rowStyle = useVirtualScroll
      ? { ...gridStyle, transform: `translateY(${i * ROW_HEIGHT}px)` }
      : gridStyle;

    visibleRows.push(
      <div
        key={i}
        class={rowClasses}
        style={rowStyle}
        role="row"
        aria-rowindex={i + 1}
      >
        <div
          class={styles.gutter}
          onClick={(e: MouseEvent) => { e.stopPropagation(); onRowClick(row); }}
          title="Expand event"
        >
          <ChevronRight size={12} />
        </div>
        {columns.map((col) => {
          const raw = row[col];
          const isTime = col === "_time";
          const display = isTime ? formatTime(raw) : truncate(raw);
          const fullValue = raw == null ? "" : String(raw);

          const cellClasses = [
            styles.cell,
            isTime ? styles.cellTime : "",
            hasTimeCol && isTime ? styles.cellSticky : "",
            effectiveIsAgg && isNumeric(raw) ? styles.cellNumber : "",
            wrap ? styles.cellWrap : "",
          ]
            .filter(Boolean)
            .join(" ");

          return (
            <div
              key={col}
              class={cellClasses}
              title={fullValue}
              role="cell"
              onClick={(e: MouseEvent) => {
                if (onCellCopy && fullValue) {
                  e.stopPropagation();
                  onCellCopy(fullValue, e.clientX, e.clientY);
                }
              }}
            >
              {wrap ? fullValue : display}
            </div>
          );
        })}
      </div>,
    );
  }

  // ---- Render ----

  return (
    <div class={styles.wrapper} role="table" aria-label="Query results">
      <div class={styles.scrollContainer} ref={scrollContainerRef} onScroll={handleScroll}>
        {/* Header row inside scroll container for sticky top:0 */}
        <div class={styles.headerRow} style={gridStyle} role="row">
          <div class={styles.gutterHeader} />
          {columns.map((col) => {
            const isSorted = currentSort?.field === col;
            const cellClasses = [
              styles.headerCell,
              hasTimeCol && col === "_time" ? styles.headerCellSticky : "",
            ]
              .filter(Boolean)
              .join(" ");

            return (
              <div
                key={col}
                class={cellClasses}
                role="columnheader"
                onClick={() => handleHeaderClick(col)}
              >
                <span>{col}</span>
                {isSorted && (
                  <span class={styles.sortIndicator}>
                    {currentSort!.direction === "asc" ? "\u25B2" : "\u25BC"}
                  </span>
                )}
                <div
                  class={`${styles.resizeHandle} ${resizingCol === col ? styles.resizeActive : ""}`}
                  onPointerDown={(e: PointerEvent) => handleResizeStart(e, col)}
                  onDblClick={(e: MouseEvent) => handleResizeDblClick(e, col)}
                />
              </div>
            );
          })}
        </div>

        {/* Scroll content area */}
        <div
          class={styles.scrollContent}
          style={useVirtualScroll ? { height: totalHeight } : undefined}
        >
          {visibleRows}
        </div>
      </div>
    </div>
  );
}
