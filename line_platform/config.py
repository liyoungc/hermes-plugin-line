"""Account and group configuration (no Hermes runtime dependency)."""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional

logger = logging.getLogger(__name__)

DEFAULT_WEBHOOK_PATH = "/webhook/line"
DEFAULT_PORT = 18791

GROUP_MODES = ("shadow", "quiet", "active")
BACKENDS = ("llm", "muse")
ENROLLMENT_ACK = ("triggered", "always", "never")

# Short names an admin can type in a DM; a full model ID is accepted too.
MODEL_ALIASES = {
    "sonnet": "claude-sonnet-5-5",
    "opus": "claude-opus-5-5",
    "haiku": "claude-haiku-5-5",
}
_MODEL_ID = re.compile(r"^claude-[a-z0-9.\-]{3,60}$")
_LABEL = re.compile(r"^[\w\-]{1,32}$")


def resolve_model(value: Any) -> Optional[str]:
    """Alias or full model ID → model ID; None when it is neither."""
    text = str(value or "").strip().lower()
    if text in MODEL_ALIASES:
        return MODEL_ALIASES[text]
    return text if _MODEL_ID.match(text) else None


def valid_label(value: Any) -> bool:
    return bool(_LABEL.match(str(value or "")))


@dataclass
class GroupConfig:
    """Per-group behaviour. Absent groups keep the legacy pass-through."""

    group_id: str
    label: str = ""
    # shadow: journal only, never reply, never reach the agent.
    # quiet:  reply only when the bot is @-mentioned.
    # active: reply on mention, name call, reply-to-bot, follow-up or rule hit.
    mode: str = "active"
    persona: Optional[str] = None
    persona_prompt: str = ""
    bot_names: list[str] = field(default_factory=list)
    triggers: list[str] = field(default_factory=list)
    triggers_require_question: bool = True
    require_enrollment: bool = True
    enrollment_ack: str = "triggered"
    kb_path: Optional[str] = None
    pinned_path: Optional[str] = None
    recent_window: int = 20
    summary_every: int = 30
    followup_window_s: int = 90
    # Standalone service only: who answers. ``llm`` calls the model directly;
    # ``muse`` queues the turn for the external agent. The admin interface can
    # override this at runtime (stored in the database).
    backend: str = "llm"
    # ``llm`` backend: which model answers (None = the service default).
    model: Optional[str] = None
    # ``muse`` backend: the Muse side chat this group is bound to (opaque ID).
    muse_chat: Optional[str] = None

    @property
    def name(self) -> str:
        return self.label or self.group_id

    def compiled_triggers(self) -> list[re.Pattern[str]]:
        cached = getattr(self, "_compiled", None)
        if cached is None:
            cached = []
            for pattern in self.triggers:
                try:
                    cached.append(re.compile(pattern, re.IGNORECASE))
                except re.error as exc:
                    logger.error("[line_platform] group %s: bad trigger regex %r: %s", self.name, pattern, exc)
            object.__setattr__(self, "_compiled", cached)
        return cached


@dataclass
class LineAccount:
    """One LINE Official Account's runtime config."""

    account_id: str = "default"
    enabled: bool = True
    channel_access_token: str = ""
    channel_secret: str = ""
    webhook_path: str = DEFAULT_WEBHOOK_PATH
    # Persona routing: bare LINE group IDs → skill name. DMs and unmatched
    # groups fall back to ``default_persona``.
    group_personas: dict[str, str] = field(default_factory=dict)
    default_persona: Optional[str] = None
    # Authorization. ``allow_from`` is the DM allow-list (bare LINE user IDs).
    # ``group_personas`` and ``groups`` keys double as the group allow-list
    # when ``group_policy == "allowlist"``.
    allow_from: list[str] = field(default_factory=list)
    dm_policy: str = "allowlist"  # "allowlist" | "open" | "pairing"
    group_policy: str = "open"  # "open" | "allowlist"
    # Outbound: use the free reply token first, fall back to Push.
    reply_first: bool = True
    reply_token_ttl_s: float = 50.0
    # Admins approve members and run DM commands. They are always DM-allowed.
    admins: list[str] = field(default_factory=list)
    bot_names: list[str] = field(default_factory=list)
    utc_offset_hours: float = 8.0
    groups: dict[str, GroupConfig] = field(default_factory=dict)

    @property
    def has_credentials(self) -> bool:
        return bool(self.channel_access_token and self.channel_secret)

    def group(self, group_id: Optional[str]) -> Optional[GroupConfig]:
        return self.groups.get(group_id) if group_id else None

    def group_allowed(self, group_id: Optional[str]) -> bool:
        if self.group_policy != "allowlist":
            return True
        return bool(group_id) and (group_id in self.group_personas or group_id in self.groups)

    def find_group(self, key: str) -> Optional[GroupConfig]:
        """Resolve a group by label or ID (admin commands)."""
        key = (key or "").strip()
        if key in self.groups:
            return self.groups[key]
        for cfg in self.groups.values():
            if cfg.label and cfg.label.lower() == key.lower():
                return cfg
        return None


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def _config_or_env(extra: dict[str, Any], keys: tuple[str, ...], env_key: str, default: str = "") -> str:
    for key in keys:
        value = extra.get(key)
        if isinstance(value, str) and value.strip():
            value = value.strip()
            # ``env:NAME`` keeps secrets out of config.yaml in multi-account setups.
            if value.startswith("env:"):
                return os.getenv(value[4:].strip(), "").strip()
            return value
    env = os.getenv(env_key, "")
    return env.strip() if env else default


def _normalize_list(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        return [part.strip() for part in raw.split(",") if part.strip()]
    if isinstance(raw, Iterable):
        return [str(item).strip() for item in raw if str(item).strip()]
    return []


def _normalize_dict(raw: Any) -> dict[str, str]:
    if isinstance(raw, dict):
        return {str(k): str(v) for k, v in raw.items() if str(k).strip()}
    return {}


def _as_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _choice(value: Any, allowed: tuple[str, ...], default: str, what: str) -> str:
    text = str(value or default).strip().lower()
    if text not in allowed:
        logger.error("[line_platform] %s=%r is not one of %s; using %r", what, value, allowed, default)
        return default
    return text


def _groups_from_extra(raw: Any, account_bot_names: list[str]) -> dict[str, GroupConfig]:
    groups: dict[str, GroupConfig] = {}
    if not isinstance(raw, dict):
        return groups
    for group_id, cfg in raw.items():
        gid = str(group_id).strip()
        if not gid:
            continue
        cfg = cfg if isinstance(cfg, dict) else {}

        def pick(*keys: str) -> Any:
            for key in keys:
                if key in cfg:
                    return cfg[key]
            return None

        mode = "quiet" if _as_bool(pick("quiet"), False) else "active"
        groups[gid] = GroupConfig(
            group_id=gid,
            label=str(pick("label", "name") or "").strip(),
            mode=_choice(pick("mode") or mode, GROUP_MODES, "active", f"groups.{gid}.mode"),
            persona=(str(pick("persona") or "").strip() or None),
            persona_prompt=str(pick("persona_prompt", "personaPrompt") or "").strip(),
            bot_names=_normalize_list(pick("bot_names", "botNames")) or list(account_bot_names),
            triggers=_normalize_list(pick("triggers")),
            triggers_require_question=_as_bool(pick("triggers_require_question"), True),
            require_enrollment=_as_bool(pick("require_enrollment", "requireEnrollment"), True),
            enrollment_ack=_choice(
                pick("enrollment_ack"), ENROLLMENT_ACK, "triggered", f"groups.{gid}.enrollment_ack"
            ),
            kb_path=(str(pick("kb", "kb_path") or "").strip() or None),
            pinned_path=(str(pick("pinned", "pinned_path") or "").strip() or None),
            recent_window=int(pick("recent_window") or 20),
            summary_every=int(pick("summary_every") or 30),
            followup_window_s=int(pick("followup_window_s") or 90),
            backend=_choice(pick("backend"), BACKENDS, "llm", f"groups.{gid}.backend"),
            model=resolve_model(pick("model")),
            muse_chat=(str(pick("muse_chat", "museChat") or "").strip() or None),
        )
    return groups


def account_from_extra(extra: dict[str, Any], account_id: str = "default") -> LineAccount:
    """Build a LineAccount from one ``extra`` block.

    Falls back to env vars (LINE_CHANNEL_ACCESS_TOKEN / LINE_CHANNEL_SECRET)
    when the block has no inline credentials. Multi-account setups should
    give each account its own values (inline, or ``env:OTHER_NAME``).
    """

    def pick(*keys: str) -> Any:
        for key in keys:
            if key in extra:
                return extra[key]
        return None

    enabled_raw = pick("enabled")
    bot_names = _normalize_list(pick("bot_names", "botNames"))
    return LineAccount(
        account_id=account_id,
        enabled=bool(enabled_raw if enabled_raw is not None else True),
        channel_access_token=_config_or_env(
            extra, ("channel_access_token", "channelAccessToken"), "LINE_CHANNEL_ACCESS_TOKEN"
        ),
        channel_secret=_config_or_env(extra, ("channel_secret", "channelSecret"), "LINE_CHANNEL_SECRET"),
        webhook_path=str(pick("webhook_path", "webhookPath") or DEFAULT_WEBHOOK_PATH),
        group_personas=_normalize_dict(pick("group_personas", "groupPersonas")),
        default_persona=(str(pick("default_persona", "defaultPersona") or "").strip() or None),
        allow_from=_normalize_list(pick("allow_from", "allowFrom")),
        dm_policy=str(pick("dm_policy", "dmPolicy") or "allowlist").lower(),
        group_policy=str(pick("group_policy", "groupPolicy") or "open").lower(),
        reply_first=_as_bool(pick("reply_first", "replyFirst"), True),
        reply_token_ttl_s=float(pick("reply_token_ttl_s") or 50.0),
        admins=_normalize_list(pick("admins")),
        bot_names=bot_names,
        utc_offset_hours=float(pick("utc_offset_hours") if pick("utc_offset_hours") is not None else 8.0),
        groups=_groups_from_extra(pick("groups"), bot_names),
    )


def accounts_from_extra(extra: Any) -> list[LineAccount]:
    extra = extra if isinstance(extra, dict) else {}
    accounts_cfg = extra.get("accounts")
    accounts: list[LineAccount] = []
    if isinstance(accounts_cfg, dict) and accounts_cfg:
        # Account-level keys override top-level keys (channel creds, policies…)
        for account_id, account_extra in accounts_cfg.items():
            merged = {k: v for k, v in extra.items() if k not in {"accounts", "host", "port"}}
            if isinstance(account_extra, dict):
                merged.update(account_extra)
            account = account_from_extra(merged, str(account_id))
            if account.enabled:
                accounts.append(account)
    else:
        account = account_from_extra(extra, "default")
        if account.enabled:
            accounts.append(account)
    # Sanity: webhook_path must be unique per account, otherwise we cannot
    # tell which account a request belongs to.
    seen_paths: dict[str, str] = {}
    for account in accounts:
        existing = seen_paths.get(account.webhook_path)
        if existing:
            logger.warning(
                "[line_platform] account %s shares webhook_path %s with %s — second registration overrides first",
                account.account_id,
                account.webhook_path,
                existing,
            )
        seen_paths[account.webhook_path] = account.account_id
    return accounts
