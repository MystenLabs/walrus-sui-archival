"use client";

import { useQuery, useInfiniteQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api, HomepageInfo, BlobsResponse, CheckpointInfo, ExpiredBlobInfo } from "./api";
import {
  getSharedFundBalance,
  getCurrentWalrusEpoch,
  contributeToSharedFund,
  extendBlobsUsingSharedFund,
} from "./wallet";

// Query keys
export const queryKeys = {
  homepage: ["homepage"] as const,
  blobs: ["blobs"] as const,
  checkpoint: (num: number, showContent: boolean) => ["checkpoint", num, showContent] as const,
  expiredBlobs: (epoch: number) => ["expiredBlobs", epoch] as const,
  sharedFundBalance: ["sharedFundBalance"] as const,
  walrusEpoch: ["walrusEpoch"] as const,
};

// Homepage info query
export function useHomepageInfo() {
  return useQuery<HomepageInfo>({
    queryKey: queryKeys.homepage,
    queryFn: () => api.getHomepageInfo(),
    staleTime: 30 * 1000, // 30 seconds
    refetchInterval: 60 * 1000, // Refetch every minute
  });
}

// Blobs infinite query with pagination
// Each request fetches 500 blobs
export function useBlobs() {
  const FETCH_SIZE = 500;

  return useInfiniteQuery<BlobsResponse>({
    queryKey: queryKeys.blobs,
    queryFn: ({ pageParam }) => {
      return api.getBlobs(FETCH_SIZE, pageParam as number | undefined);
    },
    initialPageParam: undefined as number | undefined,
    getNextPageParam: (lastPage) => lastPage.next_cursor ?? undefined,
    staleTime: 30 * 1000,
  });
}

// Checkpoint query
export function useCheckpoint(checkpointNumber: number | null, showContent: boolean = false) {
  return useQuery<CheckpointInfo>({
    queryKey: queryKeys.checkpoint(checkpointNumber ?? 0, showContent),
    queryFn: () => api.getCheckpoint(checkpointNumber!, showContent),
    enabled: checkpointNumber !== null && checkpointNumber >= 0,
    staleTime: 5 * 60 * 1000, // 5 minutes - checkpoints don't change
  });
}

// Expired blobs query
export function useExpiredBlobs(epoch: number | null) {
  return useQuery<ExpiredBlobInfo[]>({
    queryKey: queryKeys.expiredBlobs(epoch ?? 0),
    queryFn: () => api.getBlobsExpiredBeforeEpoch(epoch!),
    enabled: epoch !== null && epoch > 0,
    staleTime: 60 * 1000,
  });
}

// Shared fund balance query
export function useSharedFundBalance() {
  return useQuery<bigint>({
    queryKey: queryKeys.sharedFundBalance,
    queryFn: getSharedFundBalance,
    staleTime: 30 * 1000,
    refetchInterval: 60 * 1000,
  });
}

// Current Walrus epoch query
export function useWalrusEpoch() {
  return useQuery<number>({
    queryKey: queryKeys.walrusEpoch,
    queryFn: getCurrentWalrusEpoch,
    staleTime: 5 * 60 * 1000, // 5 minutes - epoch changes slowly
  });
}

// Contribute mutation
export function useContribute() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (amountWal: number) => contributeToSharedFund(amountWal),
    onSuccess: () => {
      // Invalidate shared fund balance to refetch
      queryClient.invalidateQueries({ queryKey: queryKeys.sharedFundBalance });
    },
  });
}

// Extend blobs mutation
export function useExtendBlobs() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async ({ objectIds, epochsToExtend = 5 }: { objectIds: string[]; epochsToExtend?: number }) => {
      const result = await extendBlobsUsingSharedFund(objectIds, epochsToExtend);
      // Also notify server to refresh blob end epochs
      await api.refreshBlobEndEpoch(objectIds);
      return result;
    },
    onSuccess: () => {
      // Invalidate queries that might be affected
      queryClient.invalidateQueries({ queryKey: queryKeys.blobs });
      queryClient.invalidateQueries({ queryKey: queryKeys.homepage });
      queryClient.invalidateQueries({ queryKey: queryKeys.sharedFundBalance });
    },
  });
}
