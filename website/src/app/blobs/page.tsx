"use client";

import { useState } from "react";
import Link from "next/link";
import { formatSize, formatAddress, formatNumber } from "@/lib/config";
import { useBlobs } from "@/lib/queries";

const PAGE_SIZE = 50;

export default function BlobsPage() {
  const [currentPage, setCurrentPage] = useState(1);
  const [jumpToPage, setJumpToPage] = useState("");

  const {
    data,
    isLoading,
    error,
    fetchNextPage,
    hasNextPage,
    isFetchingNextPage,
    refetch,
  } = useBlobs();

  // Flatten all pages of blobs
  const allBlobs = data?.pages.flatMap((page) => page.blobs) ?? [];

  // Calculate which blobs to show based on current page
  const startIndex = (currentPage - 1) * PAGE_SIZE;
  const endIndex = startIndex + PAGE_SIZE;
  const displayedBlobs = allBlobs.slice(startIndex, endIndex);

  // Calculate total pages available (from loaded data)
  const totalLoadedPages = Math.ceil(allBlobs.length / PAGE_SIZE);

  // Calculate summary stats for displayed page
  const totalEntries = displayedBlobs.reduce((sum, b) => sum + b.entries_count, 0);
  const totalSize = displayedBlobs.reduce((sum, b) => sum + b.total_size, 0);

  const handlePageChange = (page: number) => {
    if (page >= 1 && page <= totalLoadedPages) {
      setCurrentPage(page);
    }
  };

  const handleLoadMore = async () => {
    await fetchNextPage();
  };

  const handleJumpToPage = (e: React.FormEvent) => {
    e.preventDefault();
    const page = parseInt(jumpToPage, 10);
    if (!isNaN(page) && page >= 1 && page <= totalLoadedPages) {
      setCurrentPage(page);
      setJumpToPage("");
    }
  };

  if (isLoading) {
    return (
      <div className="container">
        <div className="loading">
          <div className="spinner"></div>
          <p>loading blobs...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="container">
        <div className="error">
          <h2>Error Loading Blobs</h2>
          <div className="error-message">
            {error instanceof Error ? error.message : "Failed to load blobs"}
          </div>
          <button onClick={() => refetch()} className="wallet-btn" style={{ marginTop: "20px" }}>
            Retry
          </button>
        </div>
      </div>
    );
  }

  // Generate page numbers: show sliding window of 10 pages
  const MAX_VISIBLE_PAGES = 10;
  const getVisiblePages = (): number[] => {
    if (totalLoadedPages <= MAX_VISIBLE_PAGES) {
      return Array.from({ length: totalLoadedPages }, (_, i) => i + 1);
    }

    // Calculate start of window based on current page
    let start: number;
    if (currentPage <= 6) {
      start = 1;
    } else if (currentPage >= totalLoadedPages - 4) {
      start = totalLoadedPages - MAX_VISIBLE_PAGES + 1;
    } else {
      start = currentPage - 5;
    }

    return Array.from({ length: MAX_VISIBLE_PAGES }, (_, i) => start + i);
  };

  const visiblePages = getVisiblePages();
  const showStartEllipsis = visiblePages[0] > 1;
  const showEndEllipsis = visiblePages[visiblePages.length - 1] < totalLoadedPages;

  return (
    <div className="container">
      <Link href="/" className="back-link">
        &larr; Back to Home
      </Link>

      <h1>Archived Checkpoint Blobs</h1>
      <div className="subtitle">Browse all archived checkpoint blobs</div>

      {/* Summary stats */}
      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-label">Total Blobs Loaded</div>
          <div className="stat-value">{formatNumber(allBlobs.length)}</div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Page Checkpoints</div>
          <div className="stat-value">{formatNumber(totalEntries)}</div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Page Size</div>
          <div className="stat-value">{formatSize(totalSize)}</div>
        </div>
      </div>

      {/* Pagination - Top */}
      <div className="pagination-controls">
        <div className="page-buttons">
          {/* First page + ellipsis if needed */}
          {showStartEllipsis && (
            <>
              <button
                onClick={() => handlePageChange(1)}
                className="page-btn"
              >
                1
              </button>
              <span className="page-ellipsis">...</span>
            </>
          )}

          {/* Visible page window */}
          {visiblePages.map((page) => (
            <button
              key={page}
              onClick={() => handlePageChange(page)}
              className={`page-btn ${page === currentPage ? "active" : ""}`}
            >
              {page}
            </button>
          ))}

          {/* Ellipsis + last page if needed */}
          {showEndEllipsis && (
            <>
              <span className="page-ellipsis">...</span>
              <button
                onClick={() => handlePageChange(totalLoadedPages)}
                className="page-btn"
              >
                {totalLoadedPages}
              </button>
            </>
          )}

          {/* Load More button */}
          {hasNextPage && (
            <button
              onClick={handleLoadMore}
              disabled={isFetchingNextPage}
              className="page-btn load-more"
            >
              {isFetchingNextPage ? "Loading..." : "Load More"}
            </button>
          )}
        </div>

        {/* Page jumper */}
        <form onSubmit={handleJumpToPage} className="page-jumper">
          <span>Go to:</span>
          <input
            type="number"
            value={jumpToPage}
            onChange={(e) => setJumpToPage(e.target.value)}
            placeholder={`1-${totalLoadedPages}`}
            min="1"
            max={totalLoadedPages}
          />
          <button type="submit" className="page-btn">Go</button>
        </form>
      </div>

      {/* Blobs table */}
      <table className="blobs-table">
        <thead>
          <tr>
            <th>Blob ID</th>
            <th>Object ID</th>
            <th className="text-right">Start</th>
            <th className="text-right">End</th>
            <th className="text-right">Expiry</th>
            <th className="text-right">Entries</th>
            <th className="text-right">Size</th>
          </tr>
        </thead>
        <tbody>
          {displayedBlobs.map((blob) => (
            <tr key={blob.blob_id} className={blob.end_of_epoch ? "end-of-epoch" : ""}>
              <td>
                <code>{blob.blob_id}</code>
              </td>
              <td>
                <code>{blob.object_id}</code>
              </td>
              <td className="text-right">
                <Link href={`/checkpoint?checkpoint=${blob.start_checkpoint}`}>
                  {formatNumber(blob.start_checkpoint)}
                </Link>
              </td>
              <td className="text-right">
                <Link href={`/checkpoint?checkpoint=${blob.end_checkpoint}`}>
                  {formatNumber(blob.end_checkpoint)}
                </Link>
              </td>
              <td className="text-right">{blob.expiry_epoch}</td>
              <td className="text-right">{formatNumber(blob.entries_count)}</td>
              <td className="text-right">{formatSize(blob.total_size)}</td>
            </tr>
          ))}
        </tbody>
      </table>

      {/* Pagination - Bottom */}
      <div className="pagination-controls bottom">
        <button
          onClick={() => handlePageChange(currentPage - 1)}
          disabled={currentPage === 1}
          className="page-btn nav"
        >
          &larr; Previous
        </button>

        <span className="page-info">
          Page {currentPage} of {totalLoadedPages}{hasNextPage && "+"}
        </span>

        <button
          onClick={() => handlePageChange(currentPage + 1)}
          disabled={currentPage >= totalLoadedPages}
          className="page-btn nav"
        >
          Next &rarr;
        </button>

        {/* Page jumper */}
        <form onSubmit={handleJumpToPage} className="page-jumper">
          <span>Go to:</span>
          <input
            type="number"
            value={jumpToPage}
            onChange={(e) => setJumpToPage(e.target.value)}
            placeholder={`1-${totalLoadedPages}`}
            min="1"
            max={totalLoadedPages}
          />
          <button type="submit" className="page-btn">Go</button>
        </form>
      </div>

      {/* Legend */}
      <div className="legend">
        <span className="legend-item">
          <span className="legend-color end-of-epoch"></span>
          <span>End of epoch blob</span>
        </span>
      </div>
    </div>
  );
}
