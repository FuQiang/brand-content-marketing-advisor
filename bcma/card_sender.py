"""飞书 Interactive Card 发送工具

每完成一步，通过飞书 Bot API 发送结果总结卡片。
"""

import json
import os
import urllib.request
import urllib.error
from typing import Dict, Any, Optional, Union


def _resolve_feishu_creds(
    app_id: Optional[str],
    app_secret: Optional[str],
    user_open_id: Optional[str] = None,
) -> tuple:
    """从参数或环境变量解析飞书应用凭证与用户 open_id。

    环境变量：LARK_APP_ID / LARK_APP_SECRET / LARK_USER_OPEN_ID
    全部缺失时返回空字符串，由调用方决定是否跳过卡片发送。
    """
    app_id = (app_id or os.environ.get("LARK_APP_ID", "")).strip()
    app_secret = (app_secret or os.environ.get("LARK_APP_SECRET", "")).strip()
    user_open_id = (user_open_id or os.environ.get("LARK_USER_OPEN_ID", "")).strip()
    return app_id, app_secret, user_open_id


def _get_tenant_access_token(app_id: str, app_secret: str) -> str:
    """获取飞书 tenant_access_token"""
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    data = json.dumps({"app_id": app_id, "app_secret": app_secret}).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=30) as resp:
        result = json.loads(resp.read().decode("utf-8"))
        if result.get("code") != 0:
            raise RuntimeError(f"获取 token 失败: {result}")
        return result["tenant_access_token"]


def _build_step_card(step: int, step_name: str, brand: str, result: Dict[str, Any]) -> Dict[str, Any]:
    """构建步骤完成卡片"""
    
    step_titles = {
        1: "Step 1: 加载品牌基础信息",
        2: "Step 2: 生成品牌人群画像",
        3: "Step 3: 生成产品线",
        4: "Step 4: 生成品牌 4R 策略",
        5: "Step 5: 每日精选话题筛选",
        6: "Step 6: 生成双平台内容",
    }
    
    step_emoji = {
        1: "📋",
        2: "👥",
        3: "📦",
        4: "📐",
        5: "🎯",
        6: "✨",
    }
    
    # 构建卡片内容
    elements = []
    
    # 标题区
    elements.append({
        "tag": "div",
        "text": {
            "tag": "lark_md",
            "content": f"**{step_titles.get(step, step_name)}** 完成"
        }
    })
    
    # 品牌信息
    elements.append({
        "tag": "div",
        "text": {
            "tag": "lark_md",
            "content": f"**品牌:** {brand}"
        }
    })
    
    elements.append({"tag": "hr"})
    
    # 根据步骤类型构建不同的内容展示
    if step == 1:
        # Step 1: 展示品牌 7 维度
        brand_info = result.get("brand_info", {})
        fields = [
            ("品类与价格带", brand_info.get("category_price", "-")),
            ("核心差异化", brand_info.get("differentiation", "-")),
            ("最大竞品", brand_info.get("competitors", "-")),
            ("排斥人群", brand_info.get("excluded_audience", "-")),
            ("适配人设", brand_info.get("compatible_persona", "-")),
            ("冲突人设", brand_info.get("conflict_persona", "-")),
            ("高价值场景", brand_info.get("high_value_scenes", "-")),
        ]
        if result.get("auto_generated"):
            elements.append({
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": "**[LLM 自动生成]** 品牌不在 Brand 表中，已自动创建"
                }
            })
        for name, value in fields:
            elements.append({
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**{name}:** {value}"
                }
            })
    
    elif step == 2:
        # Step 2: 展示人群画像（支持多行 persona 格式）
        audience_list = result.get("audience", [])
        if isinstance(audience_list, list):
            audience_str = ", ".join(str(a) for a in audience_list)
        else:
            audience_str = str(audience_list)
        elements.append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": f"**典型人群:** {audience_str}"
            }
        })
        # 展示各人群独立画像
        personas = result.get("personas", [])
        if personas:
            for p in personas:
                p_aud = p.get("audience", [])
                p_name = p_aud[0] if isinstance(p_aud, list) and p_aud else str(p_aud)
                motivation = str(p.get("motivation", "-"))
                if len(motivation) > 80:
                    motivation = motivation[:80] + "…"
                elements.append({
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": f"**{p_name}:** {motivation}"
                    }
                })
        else:
            elements.append({
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**消费动机:** {str(result.get('motivation', '-'))[:120]}"
                }
            })
    
    elif step == 3:
        # Step 3: 展示产品线
        created = result.get("products_created", 0)
        skipped = result.get("products_skipped", 0)
        elements.append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": f"**新建产品:** {created}，**跳过(已存在):** {skipped}"
            }
        })
        # 展示产品列表
        products = result.get("created_products", [])
        for i, p in enumerate(products[:8], 1):
            name = p.get("name", "-")
            series = p.get("series", "")
            sp = p.get("selling_point", "")
            line = f"{i}. **{name}**"
            if series:
                line += f"（{series}）"
            if sp:
                line += f" — {sp}"
            elements.append({
                "tag": "div",
                "text": {"tag": "lark_md", "content": line}
            })

    elif step == 4:
        # Step 4: 展示 4R 策略生成结果（rules_prompt 摘要：章节列表 + 头部预览）
        upserted = result.get("upserted", "unknown")
        prompt_len = result.get("rules_prompt_length", 0)
        prompt_text = (result.get("rules_prompt") or "").strip()
        elements.append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": (
                    f"**操作:** {'新建' if upserted == 'insert' else '更新'}　"
                    f"**Prompt 长度:** {prompt_len} 字符"
                ),
            },
        })
        if prompt_text:
            sections = [
                line.lstrip("# ").strip()
                for line in prompt_text.splitlines()
                if line.startswith("# ")
            ]
            if sections:
                toc = "\n".join(f"• {s}" for s in sections)
                elements.append({
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": f"**章节结构**\n{toc}"},
                })
            preview = prompt_text[:180].replace("\n", " ")
            if len(prompt_text) > 180:
                preview += "…"
            elements.append({
                "tag": "div",
                "text": {"tag": "lark_md", "content": f"**开头预览**\n> {preview}"},
            })
    
    elif step == 5:
        # Step 5: 展示话题筛选结果
        write_failed = int(result.get("write_failed", 0))
        stat_line = (
            f"**候选:** {result.get('daily_topics_total', 0)} → "
            f"**评分:** {result.get('scored_count', 0)} → "
            f"**入选:** {result.get('written_count', 0)}　"
            f"**去重跳过:** {result.get('skipped_dedup', 0)}"
        )
        if write_failed:
            stat_line += f"　⚠️ **写入失败:** {write_failed}"
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": stat_line}
        })
        # 展示精选话题列表
        topics = result.get("selected_topics", [])
        if topics:
            elements.append({"tag": "hr"})
            for i, t in enumerate(topics[:5], 1):
                topic_name = t.get("topic", "-")
                score = t.get("score", 0)
                decision = t.get("decision", "")
                reason = t.get("reason", "")
                line = f"{i}. **{topic_name}** ({score}分"
                if decision:
                    line += f"/{decision}"
                line += ")"
                if reason:
                    line += f"\n　　{reason}"
                elements.append({
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": line}
                })
    
    elif step == 6:
        # Step 6: 展示内容生成结果
        elements.append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": f"**话题数:** {result.get('topic_count', 0)}"
            }
        })
        elements.append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": f"**生成内容数:** {result.get('content_created_count', 0)}"
            }
        })
        elements.append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": f"**抖音封面:** {result.get('asset_cover_douyin_uploaded', 0)}"
            }
        })
        elements.append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": f"**小红书封面:** {result.get('asset_cover_xhs_uploaded', 0)}"
            }
        })
        elements.append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": f"**AI 视频:** {result.get('asset_video_uploaded', 0)}"
            }
        })
        # 暴露封面/视频未生成的根因（dreamina CLI 缺失/产品图缺失/生成失败 …）
        skip_reasons = result.get("asset_skip_reasons") or []
        if skip_reasons:
            elements.append({"tag": "hr"})
            elements.append({
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**⚠️ 跳过原因 ({len(skip_reasons)} 条)**"
                }
            })
            for reason in skip_reasons[:5]:
                elements.append({
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": f"• {reason}"}
                })
            if len(skip_reasons) > 5:
                elements.append({
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": f"…还有 {len(skip_reasons) - 5} 条，详见运行日志"
                    }
                })

    # 添加下一步提示
    elements.append({"tag": "hr"})
    next_steps = {
        1: "下一步: init_audience 生成品牌人群画像",
        2: "下一步: init_products 生成产品线",
        3: "下一步: init_topic_rules 生成 4R 策略",
        4: "下一步: select_topic 筛选每日话题",
        5: "下一步: generate_brand_content 生成双平台内容",
        6: "全部完成！",
    }
    elements.append({
        "tag": "div",
        "text": {
            "tag": "lark_md",
            "content": f"**{next_steps.get(step, '')}**"
        }
    })
    
    card = {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": "blue" if step < 6 else "green",
            "title": {
                "tag": "plain_text",
                "content": f"{step_emoji.get(step, '✅')} 品牌内容营销中枢"
            }
        },
        "elements": elements
    }
    
    return card


def send_step_card(
    step: int,
    step_name: str,
    brand: str,
    result: Dict[str, Any],
    app_id: str,
    app_secret: str,
    receive_id: str,
    receive_id_type: str = "open_id"
) -> bool:
    """发送步骤完成卡片
    
    Args:
        step: 步骤编号 (1-6)
        step_name: 步骤名称
        brand: 品牌名称
        result: 步骤执行结果
        app_id: 飞书 App ID
        app_secret: 飞书 App Secret
        receive_id: 接收者 ID (open_id 或 chat_id)
        receive_id_type: 接收者类型 (open_id 或 chat_id)
    
    Returns:
        是否发送成功
    """
    try:
        # 获取 token
        token = _get_tenant_access_token(app_id, app_secret)
        
        # 构建卡片
        card = _build_step_card(step, step_name, brand, result)
        
        # 发送卡片
        url = f"https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type={receive_id_type}"
        data = json.dumps({
            "receive_id": receive_id,
            "msg_type": "interactive",
            "content": json.dumps(card, ensure_ascii=False)
        }).encode("utf-8")
        
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {token}"
        }
        
        req = urllib.request.Request(url, data=data, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=30) as resp:
            resp_data = json.loads(resp.read().decode("utf-8"))
            if isinstance(resp_data, dict) and resp_data.get("code") == 0:
                return True
            else:
                print(f"发送卡片失败: {resp_data}")
                return False

    except Exception as e:
        print(f"发送卡片异常: {e}")
        return False


def send_permission_error_card(
    brand: str,
    failed_tables: dict,
    token_type: str = "no_user_token",
    app_id: Optional[str] = None,
    app_secret: Optional[str] = None,
    user_open_id: Optional[str] = None,
) -> bool:
    """发送写入权限预检失败卡片。

    app_id / app_secret / user_open_id 未显式传入时，回退到环境变量
    LARK_APP_ID / LARK_APP_SECRET / LARK_USER_OPEN_ID；仍缺失则跳过发送。

    根据 token_type 区分两种场景：
      - `no_user_token` → 完全没授权：让用户首次在 Bot 会话里发 `/feishu_auth`。
      - `user_access_token*` → 已授权但 scope 不足：让用户重新走 `/feishu_auth` 补齐 scope。
    """
    app_id, app_secret, user_open_id = _resolve_feishu_creds(app_id, app_secret, user_open_id)
    if not app_id or not app_secret or not user_open_id:
        print("跳过权限错误卡片发送：缺少 LARK_APP_ID / LARK_APP_SECRET / LARK_USER_OPEN_ID")
        return False
    is_no_token = token_type.startswith("no_user_token")
    # 如果所有表报的错都是同一条（典型的全局授权问题），合并成一行展示，不刷屏
    unique_errors = set(str(v) for v in failed_tables.values())
    merged_single_reason = next(iter(unique_errors)) if len(unique_errors) == 1 else None

    header_title = (
        "🔒 需要飞书用户授权 — 品牌内容营销中枢"
        if is_no_token
        else "🔒 品牌内容营销中枢 — 写入权限不足"
    )
    header_tpl = "orange" if is_no_token else "red"

    elements = [
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": (
                    "**Step 0: 写入权限预检** ⚠️ 未通过"
                    if is_no_token
                    else "**Step 0: 写入权限预检** ❌ 失败"
                ),
            },
        },
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": f"**品牌:** {brand}\n**当前授权状态:** `{token_type}`",
            },
        },
        {"tag": "hr"},
    ]

    if merged_single_reason:
        elements.append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": (
                    f"**无法写入 {len(failed_tables)} 张表（同一原因）:**\n"
                    + ", ".join(f"`{k}`" for k in failed_tables.keys())
                    + f"\n\n**原因:** {merged_single_reason}"
                ),
            },
        })
    else:
        table_lines = "\n".join(f"• **{k}**: {v}" for k, v in failed_tables.items())
        elements.append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": f"**无法写入以下表:**\n{table_lines}",
            },
        })

    elements.append({"tag": "hr"})
    if is_no_token:
        elements.append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": (
                    "**如何授权（30 秒搞定）:**\n"
                    "1️⃣ 在本 Bot 会话里直接发送命令：`/feishu_auth`\n"
                    "2️⃣ 按 Bot 返回的卡片点击"
                    "「前往授权」并在飞书网页确认同意 scope\n"
                    "3️⃣ 授权成功后重新跑一次 `run_all --brand \""
                    f"{brand}\"` 即可"
                ),
            },
        })
    else:
        elements.append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": (
                    "**可能原因与处理:**\n"
                    "• 你已登录，但此应用缺少对上述表的 **写 scope**\n"
                    "• 在本 Bot 会话里发送 `/feishu_auth` 重新勾选 scope 补齐；"
                    "若仍然 403，请联系应用管理员在开放平台补齐该表所属应用权限"
                ),
            },
        })

    # Action 行：给一个明显的按钮引导（发送 `/feishu_auth` 要用户手动输入，
    # 飞书卡片 action 不允许代用户发消息；这里提供「前往机器人会话」按钮作为视觉引导）
    elements.append({
        "tag": "action",
        "actions": [
            {
                "tag": "button",
                "text": {"tag": "plain_text", "content": "📘 查看授权说明"},
                "type": "primary",
                "url": "https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/authentication-management/access-token/obtain-oauth-code",
            },
        ],
    })

    card = {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": header_tpl,
            "title": {
                "tag": "plain_text",
                "content": header_title,
            },
        },
        "elements": elements,
    }

    try:
        token = _get_tenant_access_token(app_id, app_secret)
        url = f"https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=open_id"
        data = json.dumps({
            "receive_id": user_open_id,
            "msg_type": "interactive",
            "content": json.dumps(card, ensure_ascii=False),
        }).encode("utf-8")
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {token}",
        }
        req = urllib.request.Request(url, data=data, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=30) as resp:
            resp_data = json.loads(resp.read().decode("utf-8"))
            return isinstance(resp_data, dict) and resp_data.get("code") == 0
    except Exception as e:
        print(f"发送权限预检卡片异常: {e}")
        return False


def send_step_card_to_current_user(
    step: int,
    step_name: str,
    brand: str,
    result: Dict[str, Any],
    app_id: Optional[str] = None,
    app_secret: Optional[str] = None,
    user_open_id: Optional[str] = None,
) -> bool:
    """发送步骤完成卡片给当前用户。

    凭证优先级：显式参数 → 环境变量 LARK_APP_ID / LARK_APP_SECRET / LARK_USER_OPEN_ID。
    三者任一缺失则跳过发送（打印提示，不抛错）。
    """
    app_id, app_secret, user_open_id = _resolve_feishu_creds(app_id, app_secret, user_open_id)
    if not app_id or not app_secret or not user_open_id:
        print("跳过步骤卡片发送：缺少 LARK_APP_ID / LARK_APP_SECRET / LARK_USER_OPEN_ID")
        return False
    return send_step_card(
        step=step,
        step_name=step_name,
        brand=brand,
        result=result,
        app_id=app_id,
        app_secret=app_secret,
        receive_id=user_open_id,
        receive_id_type="open_id"
    )
