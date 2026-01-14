"use client";

import { useState, useEffect, Suspense } from "react";
import { useSearchParams, useRouter } from "next/navigation";
import Link from "next/link";
import { formatAddress, formatNumber, formatSize } from "@/lib/config";
import { useCheckpoint } from "@/lib/queries";

function JsonTree({ data }: { data: unknown }) {
  const [collapsed, setCollapsed] = useState<Record<string, boolean>>({});

  const toggleCollapse = (path: string) => {
    setCollapsed((prev) => ({ ...prev, [path]: !prev[path] }));
  };

  const renderValue = (value: unknown, path: string = "", depth: number = 0): React.ReactNode => {
    if (value === null) {
      return <span className="json-null">null</span>;
    }

    if (typeof value === "boolean") {
      return <span className="json-boolean">{value.toString()}</span>;
    }

    if (typeof value === "number") {
      return <span className="json-number">{value}</span>;
    }

    if (typeof value === "string") {
      return <span className="json-string">&quot;{value}&quot;</span>;
    }

    if (Array.isArray(value)) {
      if (value.length === 0) return <span>[]</span>;
      const isCollapsed = collapsed[path];
      return (
        <span>
          <span className="json-toggle" onClick={() => toggleCollapse(path)}>
            {isCollapsed ? "[+]" : "[-]"}
          </span>
          {isCollapsed ? (
            <span> [{value.length} items]</span>
          ) : (
            <>
              {"[\n"}
              {value.map((item, i) => (
                <span key={i} style={{ marginLeft: `${(depth + 1) * 20}px` }}>
                  {renderValue(item, `${path}[${i}]`, depth + 1)}
                  {i < value.length - 1 ? "," : ""}
                  {"\n"}
                </span>
              ))}
              <span style={{ marginLeft: `${depth * 20}px` }}>]</span>
            </>
          )}
        </span>
      );
    }

    if (typeof value === "object") {
      const entries = Object.entries(value as Record<string, unknown>);
      if (entries.length === 0) return <span>{"{}"}</span>;
      const isCollapsed = collapsed[path];
      return (
        <span>
          <span className="json-toggle" onClick={() => toggleCollapse(path)}>
            {isCollapsed ? "{+}" : "{-}"}
          </span>
          {isCollapsed ? (
            <span> {`{${entries.length} keys}`}</span>
          ) : (
            <>
              {"{\n"}
              {entries.map(([key, val], i) => (
                <span key={key} style={{ marginLeft: `${(depth + 1) * 20}px` }}>
                  <span className="json-key">&quot;{key}&quot;</span>: {renderValue(val, `${path}.${key}`, depth + 1)}
                  {i < entries.length - 1 ? "," : ""}
                  {"\n"}
                </span>
              ))}
              <span style={{ marginLeft: `${depth * 20}px` }}>{"}"}</span>
            </>
          )}
        </span>
      );
    }

    return <span>{String(value)}</span>;
  };

  return (
    <pre className="json-tree" style={{ whiteSpace: "pre-wrap", wordBreak: "break-all" }}>
      {renderValue(data)}
    </pre>
  );
}

function CheckpointContent() {
  const searchParams = useSearchParams();
  const router = useRouter();

  const [checkpointNumber, setCheckpointNumber] = useState(
    searchParams.get("checkpoint") || ""
  );
  const [showContent, setShowContent] = useState(
    searchParams.get("show_content") === "true"
  );
  const [queryCheckpoint, setQueryCheckpoint] = useState<number | null>(
    searchParams.get("checkpoint") ? parseInt(searchParams.get("checkpoint")!, 10) : null
  );
  const [queryShowContent, setQueryShowContent] = useState(
    searchParams.get("show_content") === "true"
  );

  // React Query hook
  const { data: checkpoint, isLoading, error } = useCheckpoint(
    queryCheckpoint,
    queryShowContent
  );

  // Update URL when search is performed
  useEffect(() => {
    if (queryCheckpoint !== null) {
      const params = new URLSearchParams();
      params.set("checkpoint", queryCheckpoint.toString());
      if (queryShowContent) {
        params.set("show_content", "true");
      }
      router.push(`/checkpoint?${params.toString()}`, { scroll: false });
    }
  }, [queryCheckpoint, queryShowContent, router]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const num = parseInt(checkpointNumber, 10);
    if (isNaN(num) || num < 0) {
      return;
    }
    setQueryCheckpoint(num);
    setQueryShowContent(showContent);
  };

  return (
    <div className="container">
      <Link href="/" className="back-link">
        &larr; Back to Home
      </Link>

      <h1>Checkpoint Lookup</h1>
      <div className="subtitle">Look up a specific checkpoint by number</div>

      {/* Search form */}
      <form onSubmit={handleSubmit} className="search-form">
        <input
          type="number"
          value={checkpointNumber}
          onChange={(e) => setCheckpointNumber(e.target.value)}
          placeholder="Enter checkpoint number"
          min="0"
        />
        <label className="checkbox-label">
          <input
            type="checkbox"
            checked={showContent}
            onChange={(e) => setShowContent(e.target.checked)}
          />
          <span>Fetch content</span>
        </label>
        <button type="submit" disabled={isLoading}>
          {isLoading ? "Loading..." : "Search"}
        </button>
      </form>

      {/* Error message */}
      {error && (
        <div className="error-message">
          {error instanceof Error ? error.message : "Failed to fetch checkpoint"}
        </div>
      )}

      {/* Checkpoint info */}
      {checkpoint && (
        <>
          <div className="checkpoint-metadata">
            <h2>Checkpoint Metadata</h2>
            <table>
              <tbody>
                <tr>
                  <td>Checkpoint Number</td>
                  <td><strong>{formatNumber(checkpoint.checkpoint_number)}</strong></td>
                </tr>
                <tr>
                  <td>Blob ID</td>
                  <td><code title={checkpoint.blob_id}>{formatAddress(checkpoint.blob_id, 16)}</code></td>
                </tr>
                <tr>
                  <td>Object ID</td>
                  <td><code title={checkpoint.object_id}>{formatAddress(checkpoint.object_id, 16)}</code></td>
                </tr>
                <tr>
                  <td>Index in Blob</td>
                  <td>{checkpoint.index}</td>
                </tr>
                <tr>
                  <td>Offset</td>
                  <td>{formatNumber(checkpoint.offset)} bytes</td>
                </tr>
                <tr>
                  <td>Length</td>
                  <td>{formatSize(checkpoint.length)}</td>
                </tr>
              </tbody>
            </table>
          </div>

          {/* Checkpoint content */}
          {checkpoint.content !== undefined && checkpoint.content !== null && (
            <div className="checkpoint-content">
              <h2>Checkpoint Content</h2>
              <JsonTree data={checkpoint.content} />
            </div>
          )}

          {/* Hint to fetch content */}
          {checkpoint.content === undefined && (
            <div className="empty-state">
              <p>Enable &quot;Fetch content&quot; to view the checkpoint data.</p>
            </div>
          )}
        </>
      )}

      {/* Empty state */}
      {!checkpoint && !isLoading && !error && (
        <div className="empty-state">
          <p>Enter a checkpoint number to search.</p>
        </div>
      )}
    </div>
  );
}

export default function CheckpointPage() {
  return (
    <Suspense
      fallback={
        <div className="container">
          <div className="loading">
            <div className="spinner"></div>
            <p>loading...</p>
          </div>
        </div>
      }
    >
      <CheckpointContent />
    </Suspense>
  );
}
