#!/usr/bin/env python3

from __future__ import annotations

import argparse
import time

from backend.app.knowledge_base import get_knowledge_base_status, sync_knowledge_base
from backend.app.services import list_opportunities


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync the chat knowledge base with visible progress.")
    parser.add_argument("--cluster-id", help="Only sync a single cluster instead of the full corpus.")
    args = parser.parse_args()

    start = time.time()

    if args.cluster_id:
        print(f"Syncing cluster {args.cluster_id}...", flush=True)
        result = sync_knowledge_base(cluster_id=args.cluster_id)
        print(f"Done in {time.time() - start:.1f}s", flush=True)
        print(result, flush=True)
        print(get_knowledge_base_status(), flush=True)
        return 0

    clusters = list_opportunities()
    total = len(clusters)
    print(f"Syncing {total} clusters into the knowledge base...", flush=True)

    for index, cluster in enumerate(clusters, start=1):
        cluster_start = time.time()
        print(f"[{index}/{total}] {cluster.subject_company_name} ({cluster.cluster_id})", flush=True)
        sync_knowledge_base(cluster_id=cluster.cluster_id)
        print(f"Completed in {time.time() - cluster_start:.1f}s", flush=True)

    print(f"Full sync finished in {time.time() - start:.1f}s", flush=True)
    print(get_knowledge_base_status(), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
