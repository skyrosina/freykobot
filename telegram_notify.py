#!/usr/bin/env python3
"""Small Telegram notifier for FreykoBot.

Usage:
    python telegram_notify.py "test message"

Environment:
    TELEGRAM_ENABLED=true
    TELEGRAM_BOT_TOKEN=123456:ABC...
    TELEGRAM_CHAT_ID=123456789
"""

from __future__ import annotations

import json
import os
import sys
import urllib.parse
import urllib.request
from dataclasses import dataclass

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass
class TelegramNotifier:
    token: str = ""
    chat_id: str = ""
    enabled: bool = False
    timeout: float = 6.0
    silent: bool = False

    @classmethod
    def from_env(cls) -> "TelegramNotifier":
        return cls(
            token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
            chat_id=os.getenv("TELEGRAM_CHAT_ID", "").strip(),
            enabled=env_bool("TELEGRAM_ENABLED", False),
            timeout=float(os.getenv("TELEGRAM_TIMEOUT", "6")),
            silent=env_bool("TELEGRAM_SILENT", False),
        )

    @property
    def configured(self) -> bool:
        return bool(self.enabled and self.token and self.chat_id)

    def send(self, text: str) -> bool:
        if not self.configured:
            return False

        text = str(text)
        if len(text) > 3900:
            text = text[:3900] + "\n...[trimmed]"

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = urllib.parse.urlencode(
            {
                "chat_id": self.chat_id,
                "text": text,
                "disable_notification": "true" if self.silent else "false",
                "disable_web_page_preview": "true",
            }
        ).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                data = json.loads(resp.read().decode("utf-8", errors="replace"))
            return bool(data.get("ok"))
        except Exception as exc:
            print(f"[telegram] send failed: {exc}")
            return False


def main() -> None:
    if load_dotenv:
        load_dotenv()

    msg = " ".join(sys.argv[1:]).strip() or "FreykoBot Telegram test"
    notifier = TelegramNotifier.from_env()
    if not notifier.configured:
        print("[telegram] disabled or missing TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID")
        raise SystemExit(1)
    ok = notifier.send(msg)
    print("[telegram] sent" if ok else "[telegram] failed")
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
