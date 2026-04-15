"""
Thin HTTP wrapper around Telegram Bot API.

Only covers what Coven needs:
  - sendMessage (to linked users)
  - getUpdates (long-polling loop in telegram_poller)
  - getMe (sanity check on startup)

Setup:
  1. DM @BotFather, /newbot, give it a name, get the token.
  2. Paste token into .env as TELEGRAM_BOT_TOKEN
  3. Also set TELEGRAM_BOT_USERNAME (without @) so the frontend can build
     the t.me/<bot>?start=<code> deep link for one-click linking.
"""

from __future__ import annotations

import httpx

from app.config import settings


class TelegramError(Exception):
    pass


class TelegramClient:
    def __init__(self, token: str | None = None):
        self.token = token or settings.telegram_bot_token
        self._http: httpx.AsyncClient | None = None

    @property
    def configured(self) -> bool:
        return bool(self.token)

    @property
    def base_url(self) -> str:
        return f"https://api.telegram.org/bot{self.token}"

    async def __aenter__(self) -> "TelegramClient":
        self._http = httpx.AsyncClient(timeout=30)
        return self

    async def __aexit__(self, *_) -> None:
        if self._http is not None:
            await self._http.aclose()
            self._http = None

    async def _post(self, method: str, json: dict) -> dict:
        assert self._http is not None, "Use 'async with TelegramClient()'"
        r = await self._http.post(f"{self.base_url}/{method}", json=json)
        r.raise_for_status()
        data = r.json()
        if not data.get("ok"):
            raise TelegramError(data.get("description") or "Telegram API error")
        return data.get("result") or {}

    async def _get(self, method: str, params: dict | None = None) -> dict:
        assert self._http is not None, "Use 'async with TelegramClient()'"
        r = await self._http.get(f"{self.base_url}/{method}", params=params)
        r.raise_for_status()
        data = r.json()
        if not data.get("ok"):
            raise TelegramError(data.get("description") or "Telegram API error")
        return data.get("result") or {}

    async def send_message(
        self,
        chat_id: int | str,
        text: str,
        *,
        parse_mode: str = "HTML",
        disable_web_page_preview: bool = True,
        reply_markup: dict | None = None,
    ) -> dict:
        body: dict = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": disable_web_page_preview,
        }
        if reply_markup is not None:
            body["reply_markup"] = reply_markup
        return await self._post("sendMessage", body)

    async def get_me(self) -> dict:
        return await self._get("getMe")

    async def answer_callback_query(
        self,
        callback_query_id: str,
        text: str | None = None,
        show_alert: bool = False,
    ) -> dict:
        body: dict = {"callback_query_id": callback_query_id}
        if text is not None:
            body["text"] = text
        if show_alert:
            body["show_alert"] = True
        return await self._post("answerCallbackQuery", body)

    async def edit_message_text(
        self,
        chat_id: int | str,
        message_id: int,
        text: str,
        *,
        parse_mode: str = "HTML",
        disable_web_page_preview: bool = True,
        reply_markup: dict | None = None,
    ) -> dict:
        body: dict = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": disable_web_page_preview,
        }
        if reply_markup is not None:
            body["reply_markup"] = reply_markup
        return await self._post("editMessageText", body)

    async def get_updates(
        self, offset: int | None = None, timeout: int = 25
    ) -> list[dict]:
        params: dict = {
            "timeout": timeout,
            "allowed_updates": '["message","callback_query"]',
        }
        if offset is not None:
            params["offset"] = offset
        assert self._http is not None
        # Long-poll: wait up to `timeout` seconds for new updates.
        r = await self._http.get(
            f"{self.base_url}/getUpdates",
            params=params,
            timeout=timeout + 10,
        )
        r.raise_for_status()
        data = r.json()
        if not data.get("ok"):
            raise TelegramError(data.get("description") or "Telegram API error")
        return data.get("result") or []


async def send_to(chat_id: int | str, text: str, **kwargs) -> None:
    """Fire-and-forget one-shot send. Swallows errors so dispatcher loops don't crash."""
    if not settings.telegram_bot_token:
        return
    try:
        async with TelegramClient() as tg:
            await tg.send_message(chat_id, text, **kwargs)
    except Exception as e:
        print(f"[telegram] send failed to {chat_id}: {e}")
