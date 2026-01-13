// API client for Walrus Sui Archival

import { config } from "./config";

export interface HomepageInfo {
  blob_count: number;
  total_checkpoints: number;
  earliest_checkpoint: number;
  latest_checkpoint: number;
  total_size: number;
  metadata_info: {
    metadata_pointer_object_id: string;
    contract_package_id: string;
    current_metadata_blob_id: string | null;
  } | null;
}

export interface BlobInfo {
  blob_id: string;
  object_id: string;
  start_checkpoint: number;
  end_checkpoint: number;
  end_of_epoch: boolean;
  expiry_epoch: number;
  is_shared_blob: boolean;
  entries_count: number;
  total_size: number;
}

export interface BlobsResponse {
  blobs: BlobInfo[];
  next_cursor: number | null;
}

export interface CheckpointInfo {
  checkpoint_number: number;
  blob_id: string;
  object_id: string;
  index: number;
  offset: number;
  length: number;
  content?: unknown;
}

export interface ExpiredBlobInfo {
  blob_id: string;
  object_id: string;
  end_epoch: number;
}

class ApiClient {
  private baseUrl: string;

  constructor() {
    this.baseUrl = config.apiEndpoint;
  }

  async getHomepageInfo(): Promise<HomepageInfo> {
    const response = await fetch(`${this.baseUrl}/v1/app_info_for_homepage`);
    if (!response.ok) {
      throw new Error(`Failed to fetch homepage info: ${response.statusText}`);
    }
    return response.json();
  }

  async getBlobs(limit?: number, cursor?: number): Promise<BlobsResponse> {
    const params = new URLSearchParams();
    if (limit) params.set("limit", limit.toString());
    if (cursor) params.set("cursor", cursor.toString());

    const url = `${this.baseUrl}/v1/app_blobs${params.toString() ? `?${params}` : ""}`;
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`Failed to fetch blobs: ${response.statusText}`);
    }
    return response.json();
  }

  async getCheckpoint(checkpoint: number, showContent: boolean = false): Promise<CheckpointInfo> {
    const params = new URLSearchParams();
    params.set("checkpoint", checkpoint.toString());
    if (showContent) params.set("show_content", "true");

    const response = await fetch(`${this.baseUrl}/v1/app_checkpoint?${params}`);
    if (!response.ok) {
      throw new Error(`Failed to fetch checkpoint: ${response.statusText}`);
    }
    return response.json();
  }

  async getBlobsExpiredBeforeEpoch(epoch: number): Promise<ExpiredBlobInfo[]> {
    const response = await fetch(
      `${this.baseUrl}/v1/app_blobs_expired_before_epoch?epoch=${epoch}`
    );
    if (!response.ok) {
      throw new Error(`Failed to fetch expired blobs: ${response.statusText}`);
    }
    return response.json();
  }

  async refreshBlobEndEpoch(objectIds: string[]): Promise<{ message: string; count: number }> {
    const response = await fetch(`${this.baseUrl}/v1/app_refresh_blob_end_epoch`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ object_ids: objectIds }),
    });
    if (!response.ok) {
      throw new Error(`Failed to refresh blob end epoch: ${response.statusText}`);
    }
    return response.json();
  }
}

export const api = new ApiClient();
