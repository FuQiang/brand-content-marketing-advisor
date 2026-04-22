"""dreamina CLI 适配层。

封装服务器本地 `dreamina` CLI（即梦官方工具）的常用生成命令，供 Step 3 产品图库
和 Step 6 封面/视频复用。所有图像/视频生成统一走本地 CLI，不再依赖 AIME image_search /
inner_skills/image-generate / user_skills/jimeng-video-generator 等外部 runtime。

约束：
- 调用前确保 `dreamina login` 已完成、`dreamina user_credit` > 0；
- 所有命令均使用 `--poll=N` 同步等待，超时则继续 query_result 轮询；
- 返回值为本地文件绝对路径，失败返回 None；调用方负责后续上传/清理。
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import tempfile
import time
from typing import Dict, List, Optional

logger = logging.getLogger("bcma.dreamina_cli")


def is_available() -> bool:
    return shutil.which("dreamina") is not None


def _download_url(url: str, download_dir: str, suffix: str) -> Optional[str]:
    try:
        import urllib.request
        os.makedirs(download_dir, exist_ok=True)
        local_path = os.path.join(download_dir, f"dreamina_dl_{int(time.time()*1000)}{suffix}")
        urllib.request.urlretrieve(url, local_path)
        if os.path.isfile(local_path) and os.path.getsize(local_path) > 0:
            return local_path
    except Exception as e:
        logger.warning("[dreamina] URL 下载失败: %s", e)
    return None


def _extract_first_media_path(result_json: Dict, download_dir: str) -> Optional[str]:
    for media_type in ("images", "videos"):
        items = result_json.get(media_type) or []
        suffix = ".png" if media_type == "images" else ".mp4"
        for item in items:
            path = item.get("path", "")
            if path and os.path.isfile(path):
                return path
            url = item.get("image_url") or item.get("video_url") or ""
            if url:
                downloaded = _download_url(url, download_dir, suffix)
                if downloaded:
                    return downloaded

    try:
        files = [
            os.path.join(download_dir, f)
            for f in os.listdir(download_dir)
            if os.path.isfile(os.path.join(download_dir, f))
        ]
        if files:
            return max(files, key=lambda p: os.path.getmtime(p))
    except Exception:
        pass

    return None


def _extract_all_media_paths(result_json: Dict, download_dir: str) -> List[str]:
    """同 `_extract_first_media_path` 但返回所有命中的本地路径（按 result_json 顺序）。"""
    paths: List[str] = []
    for media_type in ("images", "videos"):
        items = result_json.get(media_type) or []
        suffix = ".png" if media_type == "images" else ".mp4"
        for item in items:
            path = item.get("path", "")
            if path and os.path.isfile(path) and path not in paths:
                paths.append(path)
                continue
            url = item.get("image_url") or item.get("video_url") or ""
            if url:
                downloaded = _download_url(url, download_dir, suffix)
                if downloaded and downloaded not in paths:
                    paths.append(downloaded)
    return paths


def _run_and_collect(
    cmd: List[str],
    download_dir: str,
    poll_seconds: int = 120,
    collect_all: bool = False,
) -> List[str]:
    """执行 dreamina submit 命令并轮询直到拿到本地文件路径。

    collect_all=False → 返回单元素列表（兼容只取首张图/首段视频的场景）；
    collect_all=True  → 返回 result_json 中所有可下载的本地路径。
    """

    os.makedirs(download_dir, exist_ok=True)

    try:
        proc = subprocess.run(
            cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, timeout=poll_seconds + 30,
        )
    except Exception as e:
        logger.warning("[dreamina] submit 失败: %s | cmd=%s", e, " ".join(cmd[:4]))
        return []

    stdout = (proc.stdout or "").strip()
    if not stdout:
        return []

    try:
        data = json.loads(stdout)
    except json.JSONDecodeError:
        logger.warning("[dreamina] 无法解析 submit 输出: %s", stdout[:200])
        return []

    submit_id = data.get("submit_id")
    gen_status = data.get("gen_status", "")
    if not submit_id:
        logger.warning("[dreamina] submit 无 submit_id: %s", stdout[:200])
        return []

    def _harvest(rj: Dict) -> List[str]:
        if collect_all:
            return _extract_all_media_paths(rj, download_dir)
        single = _extract_first_media_path(rj, download_dir)
        return [single] if single else []

    if gen_status == "success":
        paths = _harvest(data.get("result_json") or {})
        if paths:
            return paths

    if gen_status in ("querying", "success"):
        for _ in range(30):
            try:
                qproc = subprocess.run(
                    ["dreamina", "query_result", f"--submit_id={submit_id}",
                     f"--download_dir={download_dir}"],
                    check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    text=True, timeout=15,
                )
                qdata = json.loads((qproc.stdout or "").strip())
                qstatus = qdata.get("gen_status", "")
                if qstatus == "success":
                    paths = _harvest(qdata.get("result_json") or {})
                    if paths:
                        return paths
                    break
                if qstatus == "fail":
                    logger.warning("[dreamina] 任务失败 submit_id=%s: %s",
                                   submit_id, qdata.get("fail_reason", "unknown"))
                    return []
                time.sleep(4)
            except Exception as e:
                logger.warning("[dreamina] query_result 异常: %s", e)
                time.sleep(4)

    logger.warning("[dreamina] 任务未在轮询期内完成 submit_id=%s", submit_id)
    return []


def text2image(
    prompt: str,
    ratio: str = "9:16",
    resolution: str = "2k",
    poll_seconds: int = 120,
) -> Optional[str]:
    if not is_available():
        return None
    download_dir = tempfile.mkdtemp(prefix="dreamina_t2i_")
    cmd = [
        "dreamina", "text2image",
        f"--prompt={prompt}",
        f"--ratio={ratio}",
        f"--resolution_type={resolution}",
        f"--poll={poll_seconds}",
    ]
    paths = _run_and_collect(cmd, download_dir, poll_seconds, collect_all=False)
    return paths[0] if paths else None


def text2image_batch(
    prompts: List[str],
    ratio: str = "9:16",
    resolution: str = "2k",
    poll_seconds: int = 120,
) -> List[str]:
    """批量调用 text2image，每个 prompt 提交一次任务，返回成功的本地路径列表。"""
    out: List[str] = []
    for p in prompts:
        path = text2image(p, ratio=ratio, resolution=resolution, poll_seconds=poll_seconds)
        if path:
            out.append(path)
    return out


def image2image(
    base_image_path: Optional[str],
    prompt: str,
    ratio: str = "9:16",
    resolution: str = "2k",
    poll_seconds: int = 120,
) -> Optional[str]:
    """基于产品底图生成图。底图缺失或失败时自动退回 text2image。"""
    if not is_available():
        return None
    if not base_image_path or not os.path.isfile(base_image_path):
        return text2image(prompt, ratio=ratio, resolution=resolution, poll_seconds=poll_seconds)

    download_dir = tempfile.mkdtemp(prefix="dreamina_i2i_")
    cmd = [
        "dreamina", "image2image",
        f"--images={os.path.abspath(base_image_path)}",
        f"--prompt={prompt}",
        f"--ratio={ratio}",
        f"--resolution_type={resolution}",
        f"--poll={poll_seconds}",
    ]
    paths = _run_and_collect(cmd, download_dir, poll_seconds, collect_all=False)
    if paths:
        return paths[0]
    logger.info("[dreamina] image2image 失败，兜底 text2image")
    return text2image(prompt, ratio=ratio, resolution=resolution, poll_seconds=poll_seconds)


def text2video(
    prompt: str,
    duration: int = 5,
    ratio: str = "9:16",
    model_version: str = "seedance2.0_vip",
    poll_seconds: int = 180,
) -> Optional[str]:
    if not is_available():
        return None
    download_dir = tempfile.mkdtemp(prefix="dreamina_t2v_")
    cmd = [
        "dreamina", "text2video",
        f"--prompt={prompt}",
        f"--duration={duration}",
        f"--ratio={ratio}",
        f"--model_version={model_version}",
        f"--poll={poll_seconds}",
    ]
    paths = _run_and_collect(cmd, download_dir, poll_seconds, collect_all=False)
    return paths[0] if paths else None


def _cli_main() -> int:
    """CLI 入口。SKILL.md 通过 Bash 调用本模块生成封面/视频。

    示例:
        python3 bcma/dreamina_cli.py text2image --prompt="..." --ratio=9:16
        python3 bcma/dreamina_cli.py image2image --base-image=/tmp/x.png --prompt="..." --ratio=3:4
        python3 bcma/dreamina_cli.py text2video --prompt="..." --duration=5

    成功时把本地文件绝对路径写到 stdout(最后一行);失败时非零退出码 + stderr 报错.
    """
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="dreamina CLI 适配器")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_t2i = sub.add_parser("text2image", help="Text-to-image")
    p_t2i.add_argument("--prompt", required=True)
    p_t2i.add_argument("--ratio", default="9:16")
    p_t2i.add_argument("--resolution", default="2k")
    p_t2i.add_argument("--poll", type=int, default=120)

    p_i2i = sub.add_parser("image2image", help="Image-to-image (底图缺失自动退 text2image)")
    p_i2i.add_argument("--base-image", dest="base_image", default=None)
    p_i2i.add_argument("--prompt", required=True)
    p_i2i.add_argument("--ratio", default="9:16")
    p_i2i.add_argument("--resolution", default="2k")
    p_i2i.add_argument("--poll", type=int, default=120)

    p_t2v = sub.add_parser("text2video", help="Text-to-video")
    p_t2v.add_argument("--prompt", required=True)
    p_t2v.add_argument("--duration", type=int, default=5)
    p_t2v.add_argument("--ratio", default="9:16")
    p_t2v.add_argument("--model-version", dest="model_version", default="seedance2.0_vip")
    p_t2v.add_argument("--poll", type=int, default=180)

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )

    path: Optional[str] = None
    if args.cmd == "text2image":
        path = text2image(args.prompt, ratio=args.ratio, resolution=args.resolution, poll_seconds=args.poll)
    elif args.cmd == "image2image":
        path = image2image(args.base_image, args.prompt, ratio=args.ratio, resolution=args.resolution, poll_seconds=args.poll)
    elif args.cmd == "text2video":
        path = text2video(args.prompt, duration=args.duration, ratio=args.ratio, model_version=args.model_version, poll_seconds=args.poll)

    if path and os.path.isfile(path):
        print(path)
        return 0
    print("dreamina CLI 失败: 未返回有效本地文件", file=sys.stderr)
    return 2


if __name__ == "__main__":
    import sys
    sys.exit(_cli_main())
