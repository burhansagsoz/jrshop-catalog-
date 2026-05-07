import argparse
import json
import sys
import urllib.request


def post_json(url: str, payload: dict, token: str, timeout: int = 60) -> dict:
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Import JRSHOP backup in chunked /api/orders/bulk requests."
    )
    parser.add_argument("backup_path", help="Path to backup JSON file")
    parser.add_argument("--api-base", default="http://127.0.0.1:8788", help="API base URL")
    parser.add_argument("--token", default="local-dev-token", help="Bearer token")
    parser.add_argument("--chunk-size", type=int, default=500, help="Orders per chunk (max 500)")
    args = parser.parse_args()

    chunk_size = max(1, min(500, int(args.chunk_size)))
    bulk_url = args.api_base.rstrip("/") + "/api/orders/bulk"

    with open(args.backup_path, "r", encoding="utf-8") as f:
        backup = json.load(f)

    orders = backup.get("orders") or []
    if not isinstance(orders, list) or not orders:
        print("No orders found in backup.")
        return 1

    total_saved = 0
    total_stale = 0
    total_chunks = 0

    for i in range(0, len(orders), chunk_size):
        payload = {"orders": orders[i : i + chunk_size]}
        if i == 0:
            payload.update(
                {
                    "users": backup.get("users"),
                    "suppliers": backup.get("suppliers"),
                    "templates": backup.get("templates"),
                    "settings": backup.get("settings"),
                    "notifSettings": backup.get("notifSettings"),
                    "catalog": backup.get("catalog"),
                }
            )
        result = post_json(bulk_url, payload, args.token)
        total_saved += int(result.get("saved") or 0)
        total_stale += int(result.get("staleIgnored") or 0)
        total_chunks += 1
        print(
            f"Chunk {total_chunks}: sent={len(payload['orders'])} "
            f"saved={result.get('saved', 0)} stale={result.get('staleIgnored', 0)}"
        )

    print(
        f"Done. chunks={total_chunks} orders_in_file={len(orders)} "
        f"saved={total_saved} stale={total_stale}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
