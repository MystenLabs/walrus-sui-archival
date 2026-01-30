"use client";

import Link from "next/link";
import Image from "next/image";

export default function TechPage() {
  return (
    <div className="container">
      <div className="tech-header">
        <Link href="/" className="back-link">
          ← Back to Home
        </Link>
        <h1>Technical Architecture</h1>
        <div className="subtitle">How Walrus Sui Archival Works</div>
      </div>

      {/* Open Source Repository */}
      <div className="repo-section">
        <p>
          <strong>Open Source:</strong> This application is open source and available on GitHub at{" "}
          <a
            href="https://github.com/MystenLabs/walrus-sui-archival"
            target="_blank"
            rel="noopener noreferrer"
            className="repo-link"
          >
            github.com/MystenLabs/walrus-sui-archival
          </a>
        </p>
      </div>

      {/* System Architecture Diagram */}
      <div className="architecture-diagram">
        <Image
          src="/system_architecture.png"
          alt="System Architecture"
          width={1200}
          height={800}
          style={{ width: "100%", height: "auto", maxWidth: "1200px" }}
          onError={(e) => {
            (e.target as HTMLImageElement).style.display = "none";
          }}
        />
      </div>

      {/* Overview */}
      <section className="tech-section">
        <h2>Overview</h2>
        <p>
          The Walrus Sui Archival application provides a decentralized, community-supported
          solution for archiving Sui blockchain checkpoints. The system is designed to be
          reliable, deterministic, and resilient against single points of failure.
        </p>
      </section>

      {/* Data Ingestion */}
      <section className="tech-section">
        <h2>Data Ingestion</h2>
        <p>
          The application subscribes to Sui checkpoint sources, such as the Sui ingestion
          framework, to continuously receive live checkpoints as they are produced by the
          network.
        </p>
      </section>

      {/* Blob Creation Algorithm */}
      <section className="tech-section">
        <h2>Blob Creation Algorithm</h2>
        <p>
          After accumulating checkpoints, the system creates checkpoint blobs based on a
          deterministic algorithm. A blob is created when <strong>any</strong> of the
          following conditions is met:
        </p>
        <ul>
          <li>
            <strong>Checkpoint count threshold:</strong> The accumulated checkpoints reach
            a predetermined count (X checkpoints)
          </li>
          <li>
            <strong>Size threshold:</strong> The accumulated checkpoint data exceeds a
            specified size limit (Y GB)
          </li>
          <li>
            <strong>End of epoch:</strong> An end-of-epoch checkpoint is encountered
          </li>
        </ul>
        <p>
          This deterministic algorithm ensures that if anyone creates checkpoint blobs
          following the exact same format and algorithm, <strong>identical blobs</strong>{" "}
          will be created. This property is crucial for verifiability and reproducibility.
        </p>
      </section>

      {/* Blob Storage */}
      <section className="tech-section">
        <h2>Blob Storage and Indexing</h2>
        <p>
          Once a blob is created, the server uploads it to Walrus, the decentralized storage
          network. The blob metadata, including storage information, is then recorded in the
          local database.
        </p>
        <p>
          Each blob contains an <strong>index</strong> that precisely maps checkpoint numbers
          to byte ranges within the blob. This index enables efficient retrieval of individual
          checkpoints without downloading the entire blob. Clients can use this information to
          fetch only the checkpoints they are interested in.
        </p>
      </section>

      {/* Blob Extension */}
      <section className="tech-section">
        <h2>Blob Lifetime Extension</h2>
        <p>
          A blob extender component periodically scans all checkpoint blobs and identifies
          those that are about to expire on Walrus. The system automatically extends the
          lifetime of these blobs to ensure continuous availability of the archival data.
        </p>
      </section>

      {/* Metadata Snapshots */}
      <section className="tech-section">
        <h2>Metadata Snapshots and Reliability</h2>
        <p>
          To avoid reliance on any single service provider, the application periodically takes
          snapshots of the database and creates a <strong>metadata blob</strong> that is
          stored on Walrus. This approach provides several benefits:
        </p>
        <ul>
          <li>
            <strong>Service independence:</strong> If the hosting service goes down, anyone
            can read the metadata blob from Walrus, rebuild the database, and restart the
            application from any location.
          </li>
          <li>
            <strong>Increased reliability:</strong> The system can be restored without
            depending on a specific infrastructure provider.
          </li>
          <li>
            <strong>Reduced data loss risk:</strong> The metadata is preserved on a
            decentralized network, making it highly resilient.
          </li>
        </ul>
      </section>

      {/* Read Path */}
      <section className="tech-section">
        <h2>Read Path</h2>
        <p>
          The read path is designed to be simple and non-intrusive. Anyone can query the
          application to retrieve blob information for specific checkpoints. The read path
          only requires access to:
        </p>
        <ul>
          <li>
            <strong>The database:</strong> To look up which blob contains the requested
            checkpoint
          </li>
          <li>
            <strong>Walrus:</strong> To fetch the actual checkpoint data from the blob
          </li>
        </ul>
        <p>
          The read path does not interfere with the server's ingestion and archival logic,
          allowing for high availability and scalability.
        </p>
      </section>

      {/* Shared Blob Functionality */}
      <section className="tech-section">
        <h2>Shared Blob Functionality</h2>
        <p>
          One of the unique features of this system is the <strong>shared blob</strong>{" "}
          functionality, which allows community members to help extend blob lifetimes. This
          is powered by a Move smart contract on the Sui blockchain.
        </p>
        <p>
          Community members can contribute WAL tokens to a shared fund, which is then used
          to automatically extend the lifetime of checkpoint blobs. This creates a
          community-supported archival system where:
        </p>
        <ul>
          <li>
            <strong>Distributed responsibility:</strong> No single entity bears the full cost
            of maintaining the archive.
          </li>
          <li>
            <strong>Community ownership:</strong> Anyone can contribute to the longevity of
            the archival data.
          </li>
          <li>
            <strong>Transparency:</strong> All contributions and extensions are recorded
            on-chain and are publicly visible.
          </li>
        </ul>
        <p>
          This functionality is unique to Walrus, leveraging its decentralized nature to
          enable data that can be community-supported and sponsored. The Move contract
          ensures that funds are used only for their intended purpose: extending the lifetime
          of archival blobs.
        </p>
      </section>

      {/* Determinism and Verifiability */}
      <section className="tech-section">
        <h2>Determinism and Verifiability</h2>
        <p>
          The deterministic blob creation algorithm is a key feature of the system. Because
          the algorithm is deterministic, anyone can:
        </p>
        <ul>
          <li>
            <strong>Verify integrity:</strong> Independently recreate blobs and verify that
            they match the published blobs.
          </li>
          <li>
            <strong>Reproduce the archive:</strong> Start their own archival node and produce
            identical blobs.
          </li>
          <li>
            <strong>Audit the system:</strong> Ensure that the archival process is following
            the published algorithm.
          </li>
        </ul>
      </section>

      {/* Summary */}
      <section className="tech-section">
        <h2>Summary</h2>
        <p>
          The Walrus Sui Archival application combines deterministic algorithms, decentralized
          storage, and community-driven funding to create a robust, verifiable, and resilient
          checkpoint archival system. By eliminating single points of failure and enabling
          community participation, the system ensures long-term preservation of Sui blockchain
          history.
        </p>
      </section>

      {/* Back to Home */}
      <div className="tech-footer">
        <Link href="/" className="nav-button" style={{ display: "inline-block" }}>
          <div className="nav-button-title">Back to Home</div>
          <div className="nav-button-desc">Return to the main page</div>
        </Link>
      </div>
    </div>
  );
}
