"""byted_aime_sdk - AIME SDK 兼容层

通过 OpenClaw sessions_spawn 调用系统 LLM 能力
"""

from typing import Any, Dict, List, Optional
import logging
import json
import os
import subprocess
import tempfile
import time

logger = logging.getLogger("byted_aime_sdk")


def call_aime_tool(
    toolset: str,
    tool_name: str,
    parameters: Dict[str, Any],
    response_format: str = "text"
) -> Optional[Dict[str, Any]]:
    """调用 AIME 工具 - 通过系统 LLM 实现"""
    if tool_name == "mcp:llm_chat":
        return _call_llm_chat(parameters)
    logger.warning(f"AIME 工具未实现: {toolset}/{tool_name}")
    return None


def _call_llm_chat(parameters: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """调用 LLM chat 完成 - 使用 OpenClaw sessions_spawn"""
    messages = parameters.get("messages", [])
    max_tokens = parameters.get("max_tokens", 4096)
    temperature = parameters.get("temperature", 0.7)
    
    # 构建 prompt - 取最后一条用户消息
    prompt = ""
    for msg in messages:
        role = msg.get("role", "user")
        content = msg.get("content", "")
        if role == "user":
            prompt = content
            break
    
    if not prompt:
        logger.error("No user message found in chat parameters")
        return None
    
    try:
        # 创建临时文件存储 prompt
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write(prompt)
            prompt_file = f.name
        
        # 创建临时文件存储结果
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            result_file = f.name
        
        # 创建 Python 脚本来调用 LLM
        script_content = f'''
import json
import os
import sys

# 读取 prompt
with open("{prompt_file}", "r") as f:
    prompt = f.read()

# 使用 OpenClaw 的模型调用方式
# 尝试通过 subprocess 调用 openclaw 的 internal 机制
result = ""
try:
    # 首先尝试使用环境变量中的 API key
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        # 尝试从 openclaw 配置读取
        import subprocess
        # 使用 openclaw 的内部命令获取 token
        proc = subprocess.run(
            ["openclaw", "auth", "token", "anthropic"],
            capture_output=True,
            text=True,
            timeout=10
        )
        if proc.returncode == 0:
            api_key = proc.stdout.strip()
    
    if api_key:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens={max_tokens},
            temperature={temperature},
            messages=[{{"role": "user", "content": prompt}}],
        )
        content = getattr(msg, "content", None)
        if isinstance(content, list):
            parts = []
            for block in content:
                if hasattr(block, "text"):
                    parts.append(str(getattr(block, "text", "")))
                elif isinstance(block, dict) and block.get("type") == "text":
                    parts.append(str(block.get("text", "")))
            result = "\\n".join(parts).strip()
        elif isinstance(content, str):
            result = content.strip()
        else:
            result = str(content)
    else:
        result = "ERROR: No API key available"
except Exception as e:
    result = f"ERROR: {{e}}"

# 写入结果
with open("{result_file}", "w") as f:
    json.dump({{"result": result}}, f)
'''
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write(script_content)
            script_path = f.name
        
        # 执行脚本
        result = subprocess.run(
            ['python3', script_path],
            capture_output=True,
            text=True,
            timeout=120
        )
        
        # 读取结果
        with open(result_file, 'r') as f:
            data = json.load(f)
            llm_result = data.get("result", "")
        
        # 清理临时文件
        os.unlink(prompt_file)
        os.unlink(result_file)
        os.unlink(script_path)
        
        if llm_result.startswith("ERROR:"):
            logger.error(f"LLM 调用失败: {llm_result}")
            return None
        
        return {"result": llm_result}
        
    except Exception as e:
        logger.error(f"LLM 调用异常: {e}")
        return None


def call_mcp_tool(tool_name: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """调用 MCP 工具 - 存根实现"""
    logger.warning(f"MCP 工具调用未实现: {tool_name}")
    return None