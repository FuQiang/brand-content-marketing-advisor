"""Unified LLM client that reads model config from openclaw.json.

Provider priority:
  1. openclaw.json → models.providers (bailian / volcengine, OpenAI-compatible)
  2. ANTHROPIC_API_KEY env var → Anthropic native SDK
  3. byted_aime_sdk fallback

All three callers (brand_setup, scoring, copywriting) should use the two
public functions below instead of inline LLM code.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import openai

logger = logging.getLogger("bcma.llm_client")

# ---------------------------------------------------------------------------
#  Load openclaw model providers (cached)
# ---------------------------------------------------------------------------

_OPENCLAW_PROVIDERS: Optional[Dict[str, Any]] = None


def _load_openclaw_providers() -> Dict[str, Any]:
    """Read ~/.openclaw/openclaw.json → models.providers, cached after first call."""
    global _OPENCLAW_PROVIDERS
    if _OPENCLAW_PROVIDERS is not None:
        return _OPENCLAW_PROVIDERS

    oc_path = Path.home() / ".openclaw" / "openclaw.json"
    try:
        with open(oc_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        _OPENCLAW_PROVIDERS = data.get("models", {}).get("providers", {})
    except Exception as e:
        logger.warning("Failed to load openclaw.json providers: %s", e)
        _OPENCLAW_PROVIDERS = {}
    return _OPENCLAW_PROVIDERS


def _pick_provider_and_model(
    preferred_model: str = "",
) -> Optional[Dict[str, Any]]:
    """Find a matching provider+model from openclaw config.

    优先精确匹配 preferred_model；匹配不到时自动回退到任意可用模型，
    保证不会因为 config.yaml 里写了一个本机没有的模型名就让整条 openclaw
    providers 链路失效。

    Returns dict with keys: base_url, api_key, model_id
    or None if nothing usable.
    """
    providers = _load_openclaw_providers()
    if not providers:
        return None

    preferred = preferred_model.strip().lower()

    # 1) 精确匹配 preferred_model
    if preferred:
        for pname, pcfg in providers.items():
            for m in pcfg.get("models", []):
                if m.get("id", "").lower() == preferred or m.get("name", "").lower() == preferred:
                    return {
                        "base_url": pcfg["baseUrl"],
                        "api_key": pcfg["apiKey"],
                        "model_id": m["id"],
                    }
        # 精确匹配失败，打日志但继续回退
        logger.info("openclaw providers 中未找到 '%s'，回退到任意可用模型", preferred_model)

    # 2) Fallback: prefer volcengine (doubao, faster latency) over bailian
    for pname in ("volcengine", "bailian"):
        pcfg = providers.get(pname)
        if pcfg and pcfg.get("models"):
            m = pcfg["models"][0]
            return {
                "base_url": pcfg["baseUrl"],
                "api_key": pcfg["apiKey"],
                "model_id": m["id"],
            }

    # 3) Any provider
    for pname, pcfg in providers.items():
        if pcfg.get("models"):
            m = pcfg["models"][0]
            return {
                "base_url": pcfg["baseUrl"],
                "api_key": pcfg["apiKey"],
                "model_id": m["id"],
            }

    return None


# ---------------------------------------------------------------------------
#  Public API
# ---------------------------------------------------------------------------

def call_llm_text(
    prompt: str,
    model_cfg: Optional[Dict[str, Any]] = None,
    max_tokens: int = 4096,
    temperature: float = 0.7,
) -> Optional[str]:
    """Call LLM and return plain text. Returns None on failure.

    Args:
        prompt: user message content
        model_cfg: BCMA config model section (provider, model_name, temperature...)
        max_tokens: max output tokens
        temperature: sampling temperature
    """
    if model_cfg:
        temperature = float(model_cfg.get("temperature", temperature))

    preferred_model = (model_cfg or {}).get("model_name", "")

    # --- openclaw providers (OpenAI-compatible) ---
    result = _call_openai_compatible(prompt, preferred_model, max_tokens, temperature)
    if result is not None:
        return result

    # --- Anthropic native SDK fallback ---
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if api_key:
        try:
            import anthropic
            # 只有 claude-* 模型名才能传给 Anthropic API，其他模型名用默认值
            cfg_model = (model_cfg or {}).get("model_name", "")
            anthropic_model = cfg_model if cfg_model.startswith("claude-") else "claude-sonnet-4-6"
            client = anthropic.Anthropic(api_key=api_key)
            msg = client.messages.create(
                model=anthropic_model,
                max_tokens=max_tokens,
                temperature=temperature,
                messages=[{"role": "user", "content": prompt}],
            )
            return _extract_anthropic_text(msg)
        except Exception as e:
            logger.warning("Anthropic fallback failed: %s", e)

    # --- byted_aime_sdk fallback ---
    return _call_aime_fallback(prompt, model_cfg, max_tokens, temperature)


def call_llm_json(
    prompt: str,
    model_cfg: Optional[Dict[str, Any]] = None,
    max_tokens: int = 3072,
    temperature: float = 0.8,
) -> Optional[Dict[str, Any]]:
    """Call LLM and parse JSON from response. Returns None on failure."""
    text = call_llm_text(prompt, model_cfg, max_tokens, temperature)
    if not text:
        return None
    return _parse_json_from_text(text)


# ---------------------------------------------------------------------------
#  Internal helpers
# ---------------------------------------------------------------------------

def _call_openai_compatible(
    prompt: str,
    preferred_model: str,
    max_tokens: int,
    temperature: float,
) -> Optional[str]:
    """Call OpenAI-compatible endpoint from openclaw providers."""
    info = _pick_provider_and_model(preferred_model)
    if not info:
        return None

    try:
        client = openai.OpenAI(
            base_url=info["base_url"],
            api_key=info["api_key"],
        )
        resp = client.chat.completions.create(
            model=info["model_id"],
            messages=[{"role": "user", "content": prompt}],
            max_tokens=max_tokens,
            temperature=temperature,
        )
        choice = resp.choices[0] if resp.choices else None
        if choice and choice.message and choice.message.content:
            logger.info("LLM call OK via openclaw provider, model=%s", info["model_id"])
            return choice.message.content.strip()
    except Exception as e:
        logger.warning("OpenAI-compatible call failed (model=%s): %s", info["model_id"], e)

    return None


def _extract_anthropic_text(msg: Any) -> Optional[str]:
    """Extract text from Anthropic message response."""
    content = getattr(msg, "content", None)
    if isinstance(content, list):
        parts = []
        for block in content:
            if hasattr(block, "text"):
                parts.append(str(getattr(block, "text", "")))
            elif isinstance(block, dict) and block.get("type") == "text":
                parts.append(str(block.get("text", "")))
        return "\n".join(parts).strip() or None
    if isinstance(content, str):
        return content.strip() or None
    return None


def _call_aime_fallback(
    prompt: str,
    model_cfg: Optional[Dict[str, Any]],
    max_tokens: int,
    temperature: float,
) -> Optional[str]:
    """Last resort: byted_aime_sdk."""
    try:
        from byted_aime_sdk import call_aime_tool  # type: ignore
        resp = call_aime_tool(
            toolset="llm",
            tool_name="mcp:llm_chat",
            parameters={
                "messages": [{"role": "user", "content": prompt}],
                "model": (model_cfg or {}).get("model_name", "doubao-pro-32k"),
                "max_tokens": max_tokens,
                "temperature": temperature,
            },
            response_format="text",
        )
        result_text = getattr(resp, "result", str(resp))
        return result_text.strip() if result_text else None
    except Exception as e:
        logger.warning("AIME SDK fallback failed: %s", e)
    return None


def _parse_json_from_text(text: str) -> Optional[Dict[str, Any]]:
    """Extract JSON object from LLM text (handles markdown code fences)."""
    import re
    if not text:
        return None

    # Try markdown code block first
    m = re.search(r"```(?:json)?\s*\n?([\s\S]*?)```", text)
    if m:
        text = m.group(1).strip()

    # Try direct parse
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Try finding first { ... } block
    m = re.search(r"\{[\s\S]*\}", text)
    if m:
        try:
            return json.loads(m.group())
        except json.JSONDecodeError:
            pass

    return None
