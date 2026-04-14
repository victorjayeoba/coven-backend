"""
AVE Cloud REST client.

Wraps all 12 Data API endpoints used by the Contagion Engine.
Auto-unwraps the `{status, msg, data}` envelope — callers only see `data`.
"""

import httpx

from app.config import settings


class AveAPIError(Exception):
    pass


class AveClient:
    def __init__(self, api_key: str | None = None, base_url: str | None = None):
        self.api_key = api_key or settings.ave_api_key
        self.base_url = base_url or settings.ave_base_url
        self._http: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "AveClient":
        self._http = httpx.AsyncClient(
            base_url=self.base_url,
            headers={"X-API-KEY": self.api_key},
            timeout=30.0,
        )
        return self

    async def __aexit__(self, *_) -> None:
        if self._http is not None:
            await self._http.aclose()

    # ---- internal ----

    async def _get(self, path: str, params: dict | None = None):
        assert self._http is not None, "Use 'async with AveClient()' context."
        r = await self._http.get(path, params=params)
        r.raise_for_status()
        payload = r.json()
        if payload.get("status") != 1:
            raise AveAPIError(payload.get("msg", "AVE API error"))
        return payload.get("data")

    async def _post(self, path: str, body: dict):
        assert self._http is not None, "Use 'async with AveClient()' context."
        r = await self._http.post(path, json=body)
        r.raise_for_status()
        payload = r.json()
        if payload.get("status") != 1:
            raise AveAPIError(payload.get("msg", "AVE API error"))
        return payload.get("data")

    # ---- endpoints (12 of 13) ----

    async def search_tokens(self, keyword: str, chain: str | None = None, limit: int = 100):
        params = {"keyword": keyword, "limit": limit}
        if chain:
            params["chain"] = chain
        return await self._get("/tokens", params=params)

    async def batch_prices(self, token_ids: list[str], tvl_min: int = 1000):
        return await self._post(
            "/tokens/price",
            {"token_ids": token_ids, "tvl_min": tvl_min},
        )

    async def token_detail(self, token_id: str):
        return await self._get(f"/tokens/{token_id}")

    async def trending(self, chain: str, page: int = 0, page_size: int = 50):
        return await self._get(
            "/tokens/trending",
            params={"chain": chain, "current_page": page, "page_size": page_size},
        )

    async def top100_holders(self, token_id: str):
        return await self._get(f"/tokens/top100/{token_id}")

    async def rank_topics(self):
        return await self._get("/ranks/topics")

    async def ranks(self, topic: str, limit: int = 100):
        return await self._get("/ranks", params={"topic": topic, "limit": limit})

    async def klines_by_token(self, token_id: str, interval: int = 60, limit: int = 100):
        return await self._get(
            f"/klines/token/{token_id}",
            params={"interval": interval, "limit": limit},
        )

    async def klines_by_pair(self, pair_id: str, interval: int = 60, limit: int = 100):
        return await self._get(
            f"/klines/pair/{pair_id}",
            params={"interval": interval, "limit": limit},
        )

    async def swap_transactions(
        self,
        pair_id: str,
        limit: int = 50,
        from_time: int | None = None,
        to_time: int | None = None,
        sort: str = "desc",
    ):
        params: dict = {"limit": limit, "sort": sort}
        if from_time is not None:
            params["from_time"] = from_time
        if to_time is not None:
            params["to_time"] = to_time
        return await self._get(f"/txs/{pair_id}", params=params)

    async def contract_risk(self, token_id: str):
        return await self._get(f"/contracts/{token_id}")

    async def supported_chains(self):
        return await self._get("/supported_chains")

    # ---- wallet endpoints ----

    async def smart_wallet_list(
        self,
        chain: str,
        sort: str = "total_profit_rate",
        sort_dir: str = "desc",
    ):
        """AVE's curated ranking of profitable wallets. 5 CU."""
        return await self._get(
            "/address/smart_wallet/list",
            params={"chain": chain, "sort": sort, "sort_dir": sort_dir},
        )

    async def wallet_info(self, wallet_address: str, chain: str):
        """Full P&L summary for a wallet across all tokens on a chain. 5 CU."""
        return await self._get(
            "/address/walletinfo",
            params={"wallet_address": wallet_address, "chain": chain},
        )

    async def wallet_tokens(
        self,
        wallet_address: str,
        chain: str,
        sort: str = "last_txn_time",
        sort_dir: str = "desc",
        page_size: int = 100,
        page_no: int = 1,
        hide_sold: int = 0,
        hide_small: int = 0,
    ):
        """Every token a wallet currently holds. 10 CU."""
        return await self._get(
            "/address/walletinfo/tokens",
            params={
                "wallet_address": wallet_address,
                "chain": chain,
                "sort": sort,
                "sort_dir": sort_dir,
                "pageSize": page_size,
                "pageNO": page_no,
                "hide_sold": hide_sold,
                "hide_small": hide_small,
            },
        )

    async def wallet_pnl(
        self,
        wallet_address: str,
        chain: str,
        token_address: str,
    ):
        """P&L for a wallet on a single token. 5 CU."""
        return await self._get(
            "/address/pnl",
            params={
                "wallet_address": wallet_address,
                "chain": chain,
                "token_address": token_address,
            },
        )

    async def wallet_token_tx(
        self,
        wallet_address: str,
        chain: str,
        token_address: str,
        from_time: int | None = None,
        to_time: int | None = None,
        page_size: int = 100,
        last_id: str | None = None,
    ):
        """
        Wallet's buy/sell transaction history on a specific token. 100 CU.
        Used by the backtester for behavior-based exit timing.
        """
        params: dict = {
            "wallet_address": wallet_address,
            "chain": chain,
            "token_address": token_address,
            "page_size": page_size,
        }
        if from_time is not None:
            params["from_time"] = from_time
        if to_time is not None:
            params["to_time"] = to_time
        if last_id:
            params["last_id"] = last_id
        return await self._get("/address/tx", params=params)

    # ---- pump tracking ----

    async def tokens_platform(
        self,
        tag: str = "pump_in_new",
        limit: int = 50,
        orderby: str | None = None,
    ):
        """Tokens by launch-platform pump state. 10 CU.

        Tags: meme, pump_in_hot, pump_in_new, pump_out_hot, pump_out_new
        """
        params: dict = {"tag": tag, "limit": limit}
        if orderby:
            params["orderby"] = orderby
        return await self._get("/tokens/platform", params=params)

    async def pair_detail(self, pair_id: str):
        """Pair-level details. 5 CU."""
        return await self._get(f"/pairs/{pair_id}")

    # ---- health ----

    async def ping(self) -> bool:
        try:
            await self.supported_chains()
            return True
        except Exception:
            return False
