"""
helius.py — Async Helius API client (RPC + enhanced transactions + webhooks)
"""
import asyncio
from typing import Any, Dict, List, Optional
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from loguru import logger
from config import settings, HELIUS_RPC, HELIUS_API


class HeliusClient:
    def __init__(self):
        self.api_key = settings.helius_api_key
        self.rpc_url = HELIUS_RPC
        self.api_url = HELIUS_API
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self):
        self._client = httpx.AsyncClient(timeout=30.0)
        return self

    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()

    @property
    def client(self) -> httpx.AsyncClient:
        if not self._client:
            raise RuntimeError("HeliusClient must be used as async context manager")
        return self._client

    # ─── RPC Helpers ─────────────────────────────────────────────────────────
    @retry(stop=stop_after_attempt(4), wait=wait_exponential(min=1, max=10),
           retry=retry_if_exception_type(httpx.HTTPError))
    async def _rpc(self, method: str, params: list) -> Any:
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        r = await self.client.post(self.rpc_url, json=payload)
        r.raise_for_status()
        data = r.json()
        if "error" in data:
            raise ValueError(f"RPC error: {data['error']}")
        return data.get("result")

    @retry(stop=stop_after_attempt(4), wait=wait_exponential(min=1, max=10),
           retry=retry_if_exception_type(httpx.HTTPError))
    async def _get(self, path: str, params: dict = None) -> Any:
        url = f"{self.api_url}{path}"
        if params is None:
            params = {}
        params["api-key"] = self.api_key
        r = await self.client.get(url, params=params)
        r.raise_for_status()
        return r.json()

    @retry(stop=stop_after_attempt(4), wait=wait_exponential(min=1, max=10),
           retry=retry_if_exception_type(httpx.HTTPError))
    async def _post(self, path: str, body: dict) -> Any:
        url = f"{self.api_url}{path}?api-key={self.api_key}"
        r = await self.client.post(url, json=body)
        r.raise_for_status()
        return r.json()

    # ─── Transaction History ──────────────────────────────────────────────────
    async def get_parsed_transactions(
        self, address: str, limit: int = 100, before: str = None
    ) -> List[Dict]:
        """Return enhanced/parsed transactions for a wallet address."""
        params = {"limit": limit}
        if before:
            params["before"] = before
        try:
            return await self._get(f"/addresses/{address}/transactions", params)
        except Exception as e:
            logger.warning(f"Failed to fetch txns for {address}: {e}")
            return []

    async def get_all_transactions(self, address: str, max_txns: int = 1000) -> List[Dict]:
        """Paginate through all transactions up to max_txns."""
        results = []
        before = None
        while len(results) < max_txns:
            batch = await self.get_parsed_transactions(address, limit=100, before=before)
            if not batch:
                break
            results.extend(batch)
            before = batch[-1].get("signature")
            if len(batch) < 100:
                break
            await asyncio.sleep(0.2)  # be polite to rate limits
        return results[:max_txns]

    # ─── Token Holders (Discovery) ────────────────────────────────────────────
    async def get_token_accounts_by_owner(self, mint: str, limit: int = 50) -> List[str]:
        """Get wallet addresses that hold a given token — used for co-buyer discovery."""
        try:
            result = await self._rpc("getTokenLargestAccounts", [mint])
            if not result:
                return []
            return [acc["address"] for acc in result.get("value", [])[:limit]]
        except Exception as e:
            logger.warning(f"get_token_accounts_by_owner failed for {mint}: {e}")
            return []

    # ─── Webhook Management ───────────────────────────────────────────────────
    async def list_webhooks(self) -> List[Dict]:
        try:
            return await self._get("/webhooks")
        except Exception as e:
            logger.warning(f"list_webhooks failed: {e}")
            return []

    async def create_webhook(
        self, webhook_url: str, addresses: List[str], webhook_id: str = None
    ) -> Optional[Dict]:
        body = {
            "webhookURL": webhook_url,
            "transactionTypes": ["SWAP"],
            "accountAddresses": addresses,
            "webhookType": "enhanced",
        }
        if webhook_id:
            body["webhookID"] = webhook_id
        try:
            return await self._post("/webhooks", body)
        except Exception as e:
            logger.error(f"create_webhook failed: {e}")
            return None

    async def edit_webhook(self, webhook_id: str, addresses: List[str], webhook_url: str) -> bool:
        body = {
            "webhookURL": webhook_url,
            "transactionTypes": ["SWAP"],
            "accountAddresses": addresses,
            "webhookType": "enhanced",
        }
        try:
            await self._post(f"/webhooks/{webhook_id}", body)
            return True
        except Exception as e:
            logger.error(f"edit_webhook failed: {e}")
            return False

    async def delete_webhook(self, webhook_id: str) -> bool:
        try:
            url = f"{self.api_url}/webhooks/{webhook_id}?api-key={self.api_key}"
            r = await self.client.delete(url)
            r.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"delete_webhook failed: {e}")
            return False

    # ─── Token Metadata ───────────────────────────────────────────────────────
    async def get_token_metadata(self, mint: str) -> Optional[Dict]:
        try:
            result = await self._post("/token-metadata", {"mintAccounts": [mint]})
            if result:
                return result[0]
        except Exception as e:
            logger.warning(f"get_token_metadata failed for {mint}: {e}")
        return None
