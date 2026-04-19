"""真实产品图搜索工具。

职责：
- 基于关键词（品牌 + 产品名 + 可选卖点）从公开搜索引擎抓取「真实产品图」。
- 主路径：DuckDuckGo HTML 搜索 → 抽取 Top 页面 → 解析 HTML 里的 `og:image` /
  `twitter:image` / 大尺寸 `<img>` 作为候选，下载 Top N 张。
- 兜底路径：直接请求 Bing 图片搜索页（cn.bing.com/images/search），正则抽 `murl`
  原图 URL 下载 Top N 张。

约束：
- 不依赖任何外部 API Key；全部走 stdlib `urllib`。
- 过滤过小图（<150×150）、异常 MIME、明显非图片 URL。
- 返回值是本地文件的绝对路径列表（可能少于 num），失败时返回空列表。
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import time
import urllib.parse
import urllib.request
from typing import List, Optional, Set, Tuple

logger = logging.getLogger("bcma.image_search")


_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

_MAX_PAGE_BYTES = 2_000_000
_MAX_IMG_BYTES = 5_000_000
_MIN_IMG_BYTES = 30_000
_MIN_IMG_SIDE = 400
_HTTP_TIMEOUT = 15


def _image_dimensions(data: bytes) -> Optional[Tuple[int, int]]:
    """从文件头解析 JPEG/PNG/GIF/WEBP 的 (width, height)。失败返回 None。"""
    try:
        if data[:8] == b"\x89PNG\r\n\x1a\n" and len(data) >= 24:
            w = int.from_bytes(data[16:20], "big")
            h = int.from_bytes(data[20:24], "big")
            return w, h
        if data[:3] == b"GIF" and len(data) >= 10:
            w = int.from_bytes(data[6:8], "little")
            h = int.from_bytes(data[8:10], "little")
            return w, h
        if data[:4] == b"RIFF" and data[8:12] == b"WEBP" and len(data) >= 30:
            # VP8 / VP8L / VP8X 三种常见分支，简单处理 VP8X
            if data[12:16] == b"VP8X":
                w = int.from_bytes(data[24:27], "little") + 1
                h = int.from_bytes(data[27:30], "little") + 1
                return w, h
        if data[:2] == b"\xff\xd8":
            # JPEG: 扫描 SOF 帧
            i = 2
            n = len(data)
            while i < n - 9:
                if data[i] != 0xFF:
                    i += 1
                    continue
                marker = data[i + 1]
                if marker in (0xC0, 0xC1, 0xC2, 0xC3):
                    h = int.from_bytes(data[i + 5:i + 7], "big")
                    w = int.from_bytes(data[i + 7:i + 9], "big")
                    return w, h
                seg_len = int.from_bytes(data[i + 2:i + 4], "big")
                i += 2 + seg_len
    except Exception:
        return None
    return None


def _http_get(url: str, timeout: int = _HTTP_TIMEOUT, max_bytes: int = _MAX_PAGE_BYTES) -> Optional[bytes]:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": _UA, "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read(max_bytes)
    except Exception as e:
        logger.debug("[image_search] GET 失败 url=%s err=%s", url[:120], e)
        return None


def _download_image(url: str, download_dir: str) -> Optional[str]:
    """下载单张图片到 download_dir，返回本地路径；失败返回 None。"""
    data = _http_get(url, max_bytes=_MAX_IMG_BYTES)
    if not data or len(data) < _MIN_IMG_BYTES:
        return None

    # 通过魔数识别常见图片格式
    suffix = ".jpg"
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        suffix = ".png"
    elif data[:3] == b"GIF":
        suffix = ".gif"
    elif data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        suffix = ".webp"
    elif data[:2] == b"\xff\xd8":
        suffix = ".jpg"
    else:
        # 非图片魔数，跳过
        return None

    # 过滤掉尺寸过小的图（常见缩略图、logo）
    dims = _image_dimensions(data)
    if dims is not None:
        w, h = dims
        if min(w, h) < _MIN_IMG_SIDE:
            return None

    os.makedirs(download_dir, exist_ok=True)
    name = hashlib.md5(url.encode("utf-8")).hexdigest()[:16]
    path = os.path.join(download_dir, f"imgsearch_{int(time.time()*1000)}_{name}{suffix}")
    try:
        with open(path, "wb") as f:
            f.write(data)
        return path
    except Exception as e:
        logger.debug("[image_search] 写入失败 path=%s err=%s", path, e)
        return None


# ---------- Primary: 搜索页 → 抽 og:image ----------

_DDG_HTML = "https://html.duckduckgo.com/html/?q={q}"
_RESULT_URL_RE = re.compile(r'<a[^>]+class="[^"]*result__a[^"]*"[^>]+href="([^"]+)"', re.I)
_UDDG_RE = re.compile(r"uddg=([^&]+)")
_META_IMG_RE = re.compile(
    r'<meta[^>]+(?:property|name)=["\'](?:og:image|twitter:image)["\'][^>]+content=["\']([^"\']+)["\']',
    re.I,
)
_IMG_TAG_RE = re.compile(r'<img[^>]+src=["\']([^"\']+)["\']', re.I)


def _search_page_urls(query: str, limit: int = 10) -> List[str]:
    html = _http_get(_DDG_HTML.format(q=urllib.parse.quote(query)))
    if not html:
        return []
    text = html.decode("utf-8", errors="ignore")
    urls: List[str] = []
    for m in _RESULT_URL_RE.finditer(text):
        raw = m.group(1)
        # DuckDuckGo 会把目标包在 /l/?uddg=<encoded> 里，解出来
        um = _UDDG_RE.search(raw)
        if um:
            try:
                decoded = urllib.parse.unquote(um.group(1))
                urls.append(decoded)
                continue
            except Exception:
                pass
        if raw.startswith("http"):
            urls.append(raw)
        if len(urls) >= limit:
            break
    return urls


def _extract_images_from_page(page_url: str) -> List[str]:
    html = _http_get(page_url)
    if not html:
        return []
    text = html.decode("utf-8", errors="ignore")

    out: List[str] = []
    for m in _META_IMG_RE.finditer(text):
        out.append(m.group(1))
    # 补充：<img src>，筛掉 base64/data/icon/logo/avatar
    for m in _IMG_TAG_RE.finditer(text):
        src = m.group(1)
        if not src or src.startswith("data:"):
            continue
        low = src.lower()
        if any(k in low for k in ("logo", "icon", "avatar", "sprite", "placeholder")):
            continue
        out.append(src)
        if len(out) >= 20:
            break

    # 规范化为绝对 URL
    abs_urls: List[str] = []
    for u in out:
        if u.startswith("//"):
            abs_urls.append("https:" + u)
        elif u.startswith("/"):
            parsed = urllib.parse.urlparse(page_url)
            abs_urls.append(f"{parsed.scheme}://{parsed.netloc}{u}")
        elif u.startswith("http"):
            abs_urls.append(u)
    return abs_urls


def _search_via_pages(query: str, num: int, download_dir: str) -> List[str]:
    paths: List[str] = []
    seen_urls: Set[str] = set()
    page_urls = _search_page_urls(query, limit=8)
    for page_url in page_urls:
        if len(paths) >= num:
            break
        for img_url in _extract_images_from_page(page_url):
            if img_url in seen_urls:
                continue
            seen_urls.add(img_url)
            local = _download_image(img_url, download_dir)
            if local:
                paths.append(local)
                if len(paths) >= num:
                    break
    return paths


# ---------- Fallback: Bing 图片直抓 ----------

_BING_IMG = "https://cn.bing.com/images/search?q={q}&qft=+filterui:imagesize-large&form=IRFLTR"
_MURL_RE = re.compile(r'murl&quot;:&quot;(.*?)&quot;', re.I)


def _search_bing_images(query: str, num: int, download_dir: str) -> List[str]:
    html = _http_get(_BING_IMG.format(q=urllib.parse.quote(query)))
    if not html:
        return []
    text = html.decode("utf-8", errors="ignore")
    urls: List[str] = []
    seen: Set[str] = set()
    for m in _MURL_RE.finditer(text):
        url = m.group(1).replace("\\/", "/")
        if url in seen:
            continue
        seen.add(url)
        urls.append(url)
        if len(urls) >= num * 3:
            break

    paths: List[str] = []
    for u in urls:
        local = _download_image(u, download_dir)
        if local:
            paths.append(local)
            if len(paths) >= num:
                break
    return paths


# ---------- Public API ----------

def search_real_product_images(query: str, num: int, download_dir: str) -> List[str]:
    """搜索真实产品图：优先 DuckDuckGo→页面 og:image，兜底 Bing 图片。

    返回本地文件绝对路径列表（可能少于 num），失败返回空列表。
    """
    if not query or num <= 0:
        return []

    os.makedirs(download_dir, exist_ok=True)
    paths: List[str] = []

    try:
        paths.extend(_search_via_pages(query, num, download_dir))
    except Exception as e:
        logger.warning("[image_search] 主路径异常: %s", e)

    if len(paths) < num:
        try:
            remain = num - len(paths)
            paths.extend(_search_bing_images(query, remain, download_dir))
        except Exception as e:
            logger.warning("[image_search] 兜底 Bing 异常: %s", e)

    return [os.path.abspath(p) for p in paths[:num]]
