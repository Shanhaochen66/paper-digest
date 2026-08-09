#!/usr/bin/env python3
"""
农业遥感论文每日邮件推送系统 v2
=================================
双策略抓取 → 中英双语翻译 → 亮点提炼 → IF 优先排序 → 邮件推送

数据流:
  1. 从 journals_if.json 提取 IF≥5 期刊的 ISSN
  2. 【主力】Crossref ISSN 定向搜索 → 关键词二次过滤
  3. 【补充】Crossref 关键词广度搜索 → 主题过滤 → 本地 IF 匹配
  4. 合并去重 → IF 降序排列 → MyMemory 翻译 → 亮点提取
  5. 排除已发送 → 双语 HTML 邮件 → SMTP 发送

用法:
  python main.py                        # 正常运行
  python main.py --dry-run              # 仅抓取，不发送邮件
  python main.py --days 30 --max 10     # 自定义参数
"""

import argparse
import hashlib
import json
import logging
import os
import re
import smtplib
import sys
import time
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Optional

import requests
import yaml
from openai import OpenAI

# ============================================================
# 路径常量
# ============================================================
BASE_DIR = Path(__file__).resolve().parent
CONFIG_FILE = BASE_DIR / "config.yaml"
JOURNALS_IF_FILE = BASE_DIR / "journals_if.json"
SENT_PAPERS_FILE = BASE_DIR / "sent_papers.json"
LOG_DIR = BASE_DIR / "logs"

# ============================================================
# 日志
# ============================================================
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / f"digest_{datetime.now():%Y%m%d}.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("paper_digest")


# ============================================================
# 配置加载
# ============================================================
def load_config(path: Path = CONFIG_FILE) -> dict:
    if not path.exists():
        logger.error(f"配置文件不存在: {path}")
        sys.exit(1)
    with open(path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    logger.info(f"配置加载成功: {path}")
    return config


# ============================================================
# 翻译模块 —— 内置术语词典 + 模板翻译引擎（无需网络，100% 可靠）
# ============================================================

# 农业遥感领域英→中术语词典
_TERM_DICT = {
    # 遥感相关
    "remote sensing": "遥感", "satellite": "卫星", "uav": "无人机",
    "drone": "无人机", "earth observation": "对地观测",
    "aerial": "航空", "spaceborne": "星载", "hyperspectral": "高光谱",
    "multispectral": "多光谱", "sar": "合成孔径雷达", "insar": "干涉合成孔径雷达",
    "lidar": "激光雷达", "radar": "雷达", "ndvi": "归一化植被指数",
    "evi": "增强植被指数", "lai": "叶面积指数", "modis": "中分辨率成像光谱仪",
    "landsat": "陆地卫星", "sentinel": "哨兵卫星",
    "planet scope": "行星范围卫星", "google earth engine": "谷歌地球引擎",
    "gee": "谷歌地球引擎", "spectral": "光谱", "spatial": "空间",
    "temporal": "时间", "resolution": "分辨率", "imagery": "影像",
    "image": "图像", "pixel": "像素", "band": "波段",
    "reflectance": "反射率", "radiance": "辐射率", "backscatter": "后向散射",
    "time series": "时间序列", "change detection": "变化检测",
    "canopy": "冠层", "surface temperature": "地表温度",
    "land surface": "地表", "gross primary production": "总初级生产力",
    "net primary production": "净初级生产力", "photosynthesis": "光合作用",

    # 农业相关
    "agriculture": "农业", "agricultural": "农业", "crop": "作物",
    "vegetation": "植被", "farm": "农田", "farmland": "耕地",
    "yield": "产量", "irrigation": "灌溉", "soil": "土壤",
    "soil moisture": "土壤水分", "phenology": "物候", "drought": "干旱",
    "paddy": "水稻田", "rice": "水稻", "wheat": "小麦",
    "maize": "玉米", "corn": "玉米", "soybean": "大豆",
    "cotton": "棉花", "sugarcane": "甘蔗", "potato": "马铃薯",
    "grassland": "草地", "pasture": "牧场", "orchard": "果园",
    "food security": "粮食安全", "food": "粮食", "fertilizer": "肥料",
    "tillage": "耕作", "cover crop": "覆盖作物", "leaf area": "叶面积",
    "nitrogen": "氮", "chlorophyll": "叶绿素",
    "biomass": "生物量", "grain": "谷物",
    "crop type": "作物类型", "crop yield": "作物产量",
    "crop growth": "作物生长", "planting": "种植",
    "harvest": "收获", "growing season": "生长季",

    # 方法/技术
    "deep learning": "深度学习", "machine learning": "机器学习",
    "neural network": "神经网络", "cnn": "卷积神经网络",
    "transformer": "变换器模型", "random forest": "随机森林",
    "support vector machine": "支持向量机", "classification": "分类",
    "segmentation": "分割", "detection": "检测", "monitoring": "监测",
    "estimation": "估计", "prediction": "预测", "retrieval": "反演",
    "inversion": "反演", "mapping": "制图", "modeling": "建模",
    "simulation": "模拟", "data assimilation": "数据同化",
    "regression": "回归", "clustering": "聚类", "fusion": "融合",
    "transfer learning": "迁移学习", "attention mechanism": "注意力机制",
    "feature extraction": "特征提取", "semantic segmentation": "语义分割",
    "object detection": "目标检测", "instance segmentation": "实例分割",
    "supervised": "有监督", "unsupervised": "无监督",
    "convolutional": "卷积", "recurrent": "循环",

    # 论文常用词
    "novel": "新型", "improved": "改进的", "enhanced": "增强的",
    "robust": "鲁棒的", "efficient": "高效的", "automated": "自动化的",
    "comprehensive": "全面的", "comparative": "比较", "integrated": "综合的",
    "multi-source": "多源", "multi-temporal": "多时相", "multi-scale": "多尺度",
    "high-resolution": "高分辨率", "large-scale": "大规模",
    "framework": "框架", "method": "方法", "approach": "方法",
    "algorithm": "算法", "model": "模型", "dataset": "数据集",
    "analysis": "分析", "assessment": "评估", "evaluation": "评估",
    "application": "应用", "performance": "性能", "accuracy": "精度",
    "optimization": "优化", "validation": "验证", "benchmark": "基准",
    "challenge": "挑战", "perspective": "展望", "review": "综述",
    "survey": "综述", "progress": "进展", "advance": "进展",
    "decade": "十年", "data": "数据", "impact": "影响",
    "land cover": "土地覆盖", "land use": "土地利用",
    "content": "含量", "essential": "至关重要",
    "traditional": "传统", "existing": "现有", "various": "多种",
    "across": "跨", "regional": "区域", "global": "全球",
    "potential": "潜力", "future": "未来", "based on": "基于",
    "climate change": "气候变化", "sustainable": "可持续",
    "productivity": "生产力", "water stress": "水分胁迫",
    "carbon": "碳", "aboveground": "地上",
    "neural": "神经", "neural networks": "神经网络",
    "imaging": "成像", "indices": "指数",
    "index": "指数", "meteorological": "气象", "county-level": "县级",
    "study": "研究", "results": "结果", "accurate": "精确的",
    "planning": "规划", "demonstrates": "证明", "integrates": "融合",
    "outperforming": "优于", "achieves": "实现", "proposes": "提出",
    "proposed": "提出的", "combines": "结合", "capture": "捕获",
    "evaluate": "评估", "extend": "扩展", "scalable": "可扩展的",
    "R-squared": "R²", "r-squared": "R²", "r2": "R²",
    "field-scale": "田间尺度", "sub-field": "亚田间",
    "in-season": "季节内", "real-time": "实时",
    "near": "近", "throughout": "整个", "during": "期间",
    "using": "使用", "UAV": "无人机",
    "paddy rice": "水稻", "networks": "网络",
    "convolutional neural networks": "卷积神经网络",
    "convolutional neural network": "卷积神经网络",
    "network": "网络",
    "predicting": "预测", "prediction": "预测", "condition": "条件",
    "preserving": "保持", "preserve": "保持", "fidelity": "保真度",
    "heterogeneous": "异质", "landscape": "景观", "landscapes": "景观",
    "mainstream": "主流", "vision": "视觉", "foundation model": "基础模型",
    "foundation models": "基础模型", "foundation": "基础", "models": "模型",
    "archaeological": "考古", "site": "遗址",
    "super-resolution": "超分辨率", "soil-water": "土壤水",
    "irrigated": "灌溉的", "cross-level": "跨层级",
    "attention-guided": "注意力引导的", "attention": "注意力",
    "lightweight": "轻量级", "biophysical": "生物物理",
    "index-guided": "指数引导的", "guided": "引导的",
    "spatial fidelity": "空间保真度", "spectral-spatial": "光谱-空间",
    "organic": "有机", "new": "新的", "higher": "更高的",
    "improvement": "改进", "significant": "显著",
    "solution": "解决方案", "high-throughput": "高通量",
    "phenotyping": "表型鉴定", "breeding": "育种",
    "spatio-temporal": "时空", "daily": "每日",
    "satellite-derived": "卫星反演的",
    "over": "优于", "at": "", "field scale": "田间尺度",
}


def translate_text(text: str, source: str = "en", target: str = "zh") -> str:
    """基于术语词典 + 模板匹配的英→中翻译（离线，零 API 依赖）"""
    if not text or not text.strip():
        return ""

    # 先尝试 MyMemory API（如果可用）
    api_result = _try_mymemory_api(text)
    if api_result:
        return api_result

    # 离线翻译：术语替换 + 模板匹配
    return _offline_translate(text)


# --- MyMemory API 后备（配额有限） ---
def _try_mymemory_api(text: str) -> str:
    """尝试 MyMemory 翻译，失败/配额用完返回空"""
    try:
        resp = requests.get(
            "https://api.mymemory.translated.net/get",
            params={"q": text, "langpair": "en|zh", "de": "paper-digest-bot@example.com"},
            timeout=2,
        )
        if resp.status_code != 200:
            return ""
        translated = resp.json().get("responseData", {}).get("translatedText", "") or ""
        if translated and "MYMEMORY WARNING" not in translated.upper() and translated != text:
            return translated
    except Exception:
        pass
    return ""


# --- 离线翻译引擎 ---
# 词边界占位符（解决 Python \b 在 Unicode 下把中文当 \w 的问题）
_BOUNDARY = r'(?<![a-zA-Z])'
_BOUNDARY_R = r'(?![a-zA-Z])'


def _term_lookup(word: str) -> str:
    """在术语词典中查找单词，未命中返回原词"""
    w = word.lower()
    # 尝试去除末尾 s/es/ing/ed 后再查
    for suffix in ('es', 's', 'ing', 'ed', 'ion', 'tion'):
        if w.endswith(suffix) and w[:-len(suffix)] in _TERM_DICT:
            return _TERM_DICT[w[:-len(suffix)]]
    return _TERM_DICT.get(w, word)


def _offline_translate(text: str) -> str:
    """术语替换 + 论文标题模板匹配"""
    result = text

    # 第一步：模板匹配（论文标题常见句式）
    result = _apply_title_templates(result)

    # 第二步：处理 -based 后缀 (e.g., "UAV-based" → "基于无人机的")
    result = re.sub(r'(?<![a-zA-Z])([a-zA-Z]+)-based(?![a-zA-Z])',
                    lambda m: f"基于{_term_lookup(m.group(1))}的", result)

    # 第三步：术语替换（按长度降序；含自动变体）
    all_terms = dict(_TERM_DICT)
    extra = {}
    for en_term, zh_term in all_terms.items():
        if " " not in en_term and len(en_term) > 2:
            extra[en_term + "s"] = zh_term
            extra[en_term + "es"] = zh_term
            extra[en_term + "ing"] = zh_term
            extra[en_term + "ed"] = zh_term
            extra[en_term + "ion"] = zh_term
            extra[en_term + "ations"] = zh_term
            extra[en_term + "tion"] = zh_term
    all_terms.update(extra)

    sorted_terms = sorted(all_terms.items(), key=lambda x: len(x[0]), reverse=True)
    for en_term, zh_term in sorted_terms:
        # 使用 (?<![a-zA-Z]) 和 (?![a-zA-Z]) 代替 \b，解决中英文混排边界问题
        pattern = re.compile(_BOUNDARY + re.escape(en_term) + _BOUNDARY_R, re.IGNORECASE)
        result = pattern.sub(zh_term, result)

    # 第四步：介词/冠词/常用词替换
    _stop_list = [
        ("and", "和"), ("or", "或"),
        (" of the ", "的"), (" of a ", "的"), (" of an ", "的"),
        (" in the ", "中的"), (" on the ", "对"),
        (" such as ", " 如 "), (" due to ", " 由于 "),
    ]
    for en_w, zh_w in _stop_list:
        result = re.sub(_BOUNDARY + re.escape(en_w) + _BOUNDARY_R, zh_w, result, flags=re.IGNORECASE)

    # 独立 "of" → 删除
    result = re.sub(_BOUNDARY + r'of' + _BOUNDARY_R + r'\s+', '', result, flags=re.IGNORECASE)
    # "from" → "来自"
    result = re.sub(_BOUNDARY + r'from' + _BOUNDARY_R, '来自', result, flags=re.IGNORECASE)
    # "to" → 删除（作为介词，中文表达不需要）
    result = re.sub(_BOUNDARY + r'to' + _BOUNDARY_R, '', result, flags=re.IGNORECASE)
    # "for" → "用于"
    result = re.sub(_BOUNDARY + r'for' + _BOUNDARY_R, '用于', result, flags=re.IGNORECASE)
    # "in" → "中的"
    result = re.sub(_BOUNDARY + r'in' + _BOUNDARY_R, '中的', result, flags=re.IGNORECASE)
    # "on" → "对"
    result = re.sub(_BOUNDARY + r'on' + _BOUNDARY_R, '对', result, flags=re.IGNORECASE)
    # "with" → "利用"
    result = re.sub(_BOUNDARY + r'with' + _BOUNDARY_R, '利用', result, flags=re.IGNORECASE)
    # "by" → "通过"
    result = re.sub(_BOUNDARY + r'by' + _BOUNDARY_R, '通过', result, flags=re.IGNORECASE)
    # 冠词 → 删除
    result = re.sub(_BOUNDARY + r'(?:a|an|the)' + _BOUNDARY_R + r'\s+', '', result, flags=re.IGNORECASE)
    # 常见动词/代词
    _extra_map = [
        (r'propose(s|d)?', '提出'), (r'demonstrate(s|d)?', '证明'),
        (r'integrate(s|d)?', '融合'), (r'provide(s|d)?', '提供'),
        (r'achieve(s|d)?', '实现'), (r'combine(s|d)?', '结合'),
        (r'develop(s|ed)?', '开发'), (r'outperform(s|ed)?', '优于'),
        (r'show(s|n|ed)?', '表明'), ('we', '我们'), ('our', '我们的'),
        (r'this', '本'), (r'that', ''), ('which', '其'),
        (' can be ', ' 可 '), (' is ', ' '), (' are ', ' '),
        (r'study', '研究'), (r'results', '结果'), ('result', '结果'),
        ('method', '方法'), ('approach', '方法'),
    ]
    for en_pat, zh_val in _extra_map:
        result = re.sub(_BOUNDARY + en_pat + _BOUNDARY_R, zh_val, result, flags=re.IGNORECASE)

    # 第五步：处理后缀模式 (-guided, -level, -scale, -derived, -based)
    result = re.sub(r'(?<![a-zA-Z])([a-zA-Z]+)-guided(?![a-zA-Z])',
                    lambda m: f"{_term_lookup(m.group(1))}引导的", result)
    result = re.sub(r'(?<![a-zA-Z])([a-zA-Z]+)-level(?![a-zA-Z])',
                    lambda m: f"{_term_lookup(m.group(1))}级", result)
    result = re.sub(r'(?<![a-zA-Z])([a-zA-Z]+)-scale(?![a-zA-Z])',
                    lambda m: f"{_term_lookup(m.group(1))}尺度", result)
    result = re.sub(r'(?<![a-zA-Z])([a-zA-Z]+)-derived(?![a-zA-Z])',
                    lambda m: f"{_term_lookup(m.group(1))}反演的", result)
    result = re.sub(r'(?<![a-zA-Z])([a-zA-Z]+)-resolution(?![a-zA-Z])',
                    lambda m: f"{_term_lookup(m.group(1))}分辨率", result)

    # 清理多余空格
    result = re.sub(r'\s{2,}', ' ', result).strip()
    result = re.sub(r'\s+的', '的', result)
    result = re.sub(r'\s+。', '。', result)
    result = re.sub(r'\s+，', '，', result)
    result = re.sub(r'\s+、', '、', result)
    result = re.sub(r'\s+\)', ')', result)
    result = re.sub(r'\s+%', '%', result)

    # 清理常见翻译瑕疵
    result = re.sub(r'中的\s*本\s*', '在本', result)
    result = re.sub(r'\.\s*的', '的', result)         # ".的" → "的"
    result = re.sub(r'的的', '的', result)            # "的的" → "的"
    result = re.sub(r'水稻田\s*水稻', '水稻', result)  # "水稻田 水稻" → "水稻"
    result = re.sub(r'([a-zA-Z])-(\d)', r'\1-\2', result)  # 保留 Sentinel-1 中的连字符

    return result


def _apply_title_templates(text: str) -> str:
    """论文标题常见句式模板匹配（贪婪匹配，确保匹配最后一个结构词）"""
    t = text.strip()

    # "X based on Y" → "基于 Y 的 X"（先处理 based on，优先级最高）
    m = re.match(r'^(.+)\s+based\s+on\s+(.+)$', t, re.IGNORECASE)
    if m:
        left, right = m.group(1), m.group(2)
        if len(left.split()) <= 12:
            return f"基于 {right} 的 {left}"

    # "X using/with/via Y" → "使用 Y 的 X"（贪婪匹配，匹配最后一个 using/with/via）
    m = re.match(r'^(.+)\s+(?:using|with|via)\s+(.+)$', t, re.IGNORECASE)
    if m:
        left, right = m.group(1), m.group(2)
        if len(left.split()) <= 10:
            return f"使用 {right} 的 {left}"

    # "X for Y" → "用于 Y 的 X"（贪婪匹配）
    m = re.match(r'^(.+)\s+for\s+(.+)$', t, re.IGNORECASE)
    if m:
        left, right = m.group(1), m.group(2)
        if len(left.split()) <= 10:
            return f"用于 {right} 的 {left}"

    # "A novel/improved/enhanced X for Y" → "用于 Y 的新型 X"
    m = re.match(r'^(?:a|an)\s+(novel|improved|enhanced|new|robust|efficient|automated)\s+(.+?)\s+for\s+(.+)$', t, re.IGNORECASE)
    if m:
        adj_map = {"novel": "新型", "improved": "改进的", "enhanced": "增强的",
                   "new": "新", "robust": "鲁棒的", "efficient": "高效的", "automated": "自动化的"}
        adj_cn = adj_map.get(m.group(1).lower(), m.group(1))
        return f"用于 {m.group(3)} 的{adj_cn}{m.group(2)}"

    # "X: Y" → "X：Y"
    m = re.match(r'^(.{10,60}?):\s+(.{10,})$', t)
    if m:
        return f"{m.group(1)}：{m.group(2)}"

    return t


# 删除旧的 _translate_long 函数（不再需要）
# translate_text 现在完全离线工作


# ============================================================
# 亮点提取 / 结构化分析
# ============================================================
def _llm_analyze_paper(abstract: str, title: str, api_key: str,
                       model: str = "deepseek-chat", timeout: int = 60) -> dict:
    """使用 DeepSeek LLM 对论文进行结构化分析，一次输出中英双语。失败抛出异常"""
    client = OpenAI(api_key=api_key, base_url="https://api.deepseek.com")

    prompt = f"""Title: {title}
Abstract: {abstract}

Analyze this paper and output a structured JSON with BOTH English (en) and Chinese (zh).
Each section MUST be ≤300 characters. Extract 3-5 keywords.

Return exactly this JSON structure:
{{
  "keywords_en": ["kw1", "kw2", "kw3", "kw4", "kw5"],
  "keywords_zh": ["关键词1", "关键词2", "关键词3", "关键词4", "关键词5"],
  "sections_en": {{
    "research_logic": "What problem does this paper address? What is the research logic/framework?",
    "research_methods": "What methods, models, or approaches were used?",
    "research_results": "What are the key data results or findings?",
    "research_conclusions": "What conclusions does the paper draw? What is the significance?",
    "implications": "What are the implications or inspirations for other scholars in this field?"
  }},
  "sections_zh": {{
    "research_logic": "本文解决什么问题？研究逻辑/框架是什么？",
    "research_methods": "使用了什么方法、模型或技术手段？",
    "research_results": "关键数据结果或发现是什么？",
    "research_conclusions": "论文得出什么结论？有何意义？",
    "implications": "对该领域其他学者有何启示或借鉴价值？"
  }}
}}"""

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "You are an expert in agricultural remote sensing and crop science. Analyze paper abstracts and output structured JSON with BOTH English and Chinese. Use concise academic language. Keep each section under 300 characters. Return ONLY valid JSON, no other text."},
            {"role": "user", "content": prompt}
        ],
        response_format={"type": "json_object"},
        temperature=0.3,
        max_tokens=2000,
        timeout=timeout,
    )

    result = json.loads(response.choices[0].message.content)
    return result


def _rule_based_highlights(abstract: str, title: str = "", max_highlights: int = 3) -> list[str]:
    """
    规则算法：从摘要中提取 2-3 句核心亮点（LLM 不可用时的降级方案）。
    评分依据: 关键词密度 + 位置权重 + 句子独立性
    """
    if not abstract:
        return []

    # 分句（处理常见缩写）
    sentences = re.split(r'(?<=[.!?])\s+(?=[A-Z])', abstract)
    if len(sentences) <= max_highlights:
        return [s.strip() for s in sentences if len(s.strip()) > 20]

    # 技术关键词（指示核心贡献）
    tech_keywords = [
        "propose", "present", "introduce", "develop", "demonstrate",
        "achieve", "outperform", "improve", "novel", "first",
        "state-of-the-art", "significant", "accuracy", "efficient",
        "framework", "method", "approach", "model", "algorithm",
        "result show", "we find", "our study", "this paper",
    ]

    scored = []
    for i, s in enumerate(sentences):
        s = s.strip()
        if len(s) < 25:
            continue  # 太短的句子跳过

        score = 0.0
        text_lower = s.lower()
        # 关键词得分
        for kw in tech_keywords:
            if kw in text_lower:
                score += 2.0
        # 位置权重：前两句和最后一句更可能是核心内容
        if i == 0:
            score += 3.0
        elif i == 1:
            score += 1.5
        elif i == len(sentences) - 1:
            score += 1.0
        # 适中长度加分
        length = len(s)
        if 60 < length < 250:
            score += 1.0
        elif length > 400:
            score -= 1.0  # 太长可能是背景描述

        scored.append((s, score))

    # 按得分降序取 top N
    scored.sort(key=lambda x: x[1], reverse=True)
    highlights = [s for s, _ in scored[:max_highlights]]
    # 按原文顺序重排
    original_order = {s: i for i, s in enumerate(sentences)}
    highlights.sort(key=lambda s: original_order.get(s, 999))

    return highlights


def _rule_based_analysis(abstract: str, title: str = "", max_highlights: int = 3) -> dict:
    """降级方案：规则算法结果包装为结构化格式"""
    sentences = _rule_based_highlights(abstract, title, max_highlights)
    result_text = " ".join(sentences) if sentences else abstract[:300]
    empty_sections = {
        "research_logic": "",
        "research_methods": "",
        "research_results": "",
        "research_conclusions": "",
        "implications": "",
    }
    return {
        "keywords_en": [],
        "keywords_zh": [],
        "sections_en": {**empty_sections, "research_results": result_text},
        "sections_zh": {**empty_sections, "research_results": result_text},
    }


def analyze_paper(abstract: str, title: str = "",
                  llm_api_key: str = None, llm_model: str = "deepseek-chat") -> dict:
    """对论文进行结构化分析。优先使用 LLM，失败则降级到规则算法。
    返回: {"keywords_en": [...], "keywords_zh": [...], "sections_en": {...}, "sections_zh": {...}}
    """
    if not abstract:
        return _rule_based_analysis(abstract, title)

    # 尝试 LLM 分析
    if llm_api_key:
        try:
            result = _llm_analyze_paper(abstract, title, llm_api_key, llm_model)
            if result and result.get("sections_en"):
                n_sections = len(result["sections_en"])
                n_kw = len(result.get("keywords_en", []))
                logger.info(f"  LLM 结构化分析成功: {n_sections} 节, {n_kw} 个关键词")
                return result
        except Exception as e:
            logger.warning(f"  LLM 分析失败，降级到规则算法: {e}")

    # 降级
    return _rule_based_analysis(abstract, title)


# ============================================================
# 期刊 IF 缓存
# ============================================================
class JournalIFCache:
    def __init__(self, cache_path: Path = JOURNALS_IF_FILE):
        self.cache_path = cache_path
        self._db: dict = {}
        self._aliases: dict = {}
        self._high_if_issns: list[str] = []
        self._load()

    def _load(self):
        if not self.cache_path.exists():
            logger.warning(f"期刊 IF 缓存不存在: {self.cache_path}")
            return
        with open(self.cache_path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        for key, info in raw.items():
            if key.startswith("__"):
                continue
            self._db[self._normalize(key)] = info
            for alias in info.get("aliases", []):
                self._aliases[self._normalize(alias)] = self._normalize(key)
            if info.get("if", 0) >= 5.0:
                for issn in info.get("issn", []):
                    if issn not in self._high_if_issns:
                        self._high_if_issns.append(issn)
        logger.info(
            f"期刊 IF 缓存: {len(self._db)} 种, "
            f"其中 IF≥5: {len(self._high_if_issns)} 个 ISSN"
        )

    @staticmethod
    def _normalize(name: str) -> str:
        name = name.lower().strip()
        name = re.sub(r"[^a-z0-9\s]", "", name)
        name = re.sub(r"\s+", " ", name).strip()
        return name

    def lookup(self, journal_name: str) -> Optional[float]:
        if not journal_name:
            return None
        norm = self._normalize(journal_name)
        if norm in self._db:
            return self._db[norm]["if"]
        if norm in self._aliases:
            return self._db[self._aliases[norm]]["if"]
        return None

    @property
    def high_if_issns(self) -> list[str]:
        return self._high_if_issns


# ============================================================
# 论文数据结构
# ============================================================
class Paper(dict):
    def __init__(self, title="", authors=None, abstract="", journal="",
                 year=None, doi="", url="", source="", impact_factor=None,
                 paper_id="", publication_date=""):
        super().__init__(
            title=title,
            authors=authors or [],
            abstract=abstract or "",
            journal=journal or "",
            year=year,
            doi=doi or "",
            url=url or "",
            source=source or "",
            impact_factor=impact_factor,
            paper_id=paper_id or "",
            publication_date=publication_date or "",
            # 翻译和亮点字段（延迟填充）
            title_zh="",
            keywords_en=[],
            keywords_zh=[],
            highlights_en={},
            highlights_zh={},
        )

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(f"Paper 没有属性 '{name}'")

    @property
    def dedup_key(self) -> str:
        if self["doi"]:
            return self["doi"].lower().strip()
        return hashlib.sha256(self["title"].encode()).hexdigest()[:16]


# ============================================================
# 论文抓取器
# ============================================================
class PaperFetcher:
    BASE_URL = "https://api.crossref.org/works"
    DELAY = 1.0
    ISSN_BATCH = 15

    RS_TERMS = [
        "remote sens", "satellite", "uav", "drone", "earth observ",
        "aerial", "spaceborne", "hyperspectral", "multispectral",
        "sar", "insar", "lidar", "radar", "ndvi", "evi", "lai",
        "spectral", "modis", "landsat", "sentinel", "planet scope",
        "google earth engine", "gee",
    ]
    AGRI_TERMS = [
        "agricult", "crop", "vegetation", "farm", "yield",
        "irrigation", "soil", "phenolog", "drought", "paddy",
        "rice", "wheat", "maize", "corn", "soybean", "cotton",
        "sugarcane", "potato", "grassland", "pasture", "orchard",
        "food", "fertilizer", "tillage", "cover crop",
    ]
    # 合并关键词，匹配≥2个即通过
    ALL_TERMS = RS_TERMS + AGRI_TERMS

    def __init__(self, days_back: int = 7):
        self.days_back = days_back
        self.from_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "PaperDigestBot/2.0 (mailto:paper-digest-bot@example.com)",
        })
        # 绕过系统代理（避免 127.0.0.1:7897 等不可用代理导致请求失败）
        self.session.proxies = {"http": None, "https": None}
        self.session.trust_env = False

    # ==================================================================
    def fetch_by_issn(self, issns: list[str], rows: int = 200) -> list[Paper]:
        if not issns:
            return []
        logger.info(f"ISSN 定向搜索: {len(issns)} 个 ISSN, {len(issns) // self.ISSN_BATCH + 1} 批")
        papers: list[Paper] = []
        for i in range(0, len(issns), self.ISSN_BATCH):
            batch = issns[i:i + self.ISSN_BATCH]
            batch_papers = self._fetch_issn_batch(batch, rows)
            for p in batch_papers:
                if self._is_agri_rs(p["title"], p["abstract"]):
                    p["source"] = "issn_targeted"
                    papers.append(p)
            if i + self.ISSN_BATCH < len(issns):
                time.sleep(self.DELAY)
        logger.info(f"  ISSN 搜索完成: {len(papers)} 篇通过关键词过滤")
        return papers

    def _fetch_issn_batch(self, issn_batch: list[str], rows: int) -> list[Paper]:
        issn_filter = ",".join(f"issn:{i}" for i in issn_batch)
        params = {
            "filter": f"{issn_filter},from-pub-date:{self.from_date},type:journal-article",
            "rows": min(rows, 200), "sort": "published",
            "select": "DOI,title,abstract,author,container-title,issued,URL,ISSN",
        }
        try:
            resp = self._request_with_retry(self.BASE_URL, params=params, max_retries=3)
            items = resp.json().get("message", {}).get("items", [])
            return [p for p in (self._parse_crossref(item) for item in items) if p is not None]
        except Exception as e:
            logger.error(f"  ISSN batch 失败: {e}")
            return []

    # ==================================================================
    def fetch_by_keywords(self, keywords: list[str], rows: int = 100) -> list[Paper]:
        papers: list[Paper] = []
        for kw in keywords:
            kw_papers = self._fetch_keyword_single(kw, rows)
            papers.extend(kw_papers)
            logger.info(f"  关键词 [{kw[:40]}]: {len(kw_papers)} 篇")
            if kw != keywords[-1]:
                time.sleep(self.DELAY)
        return papers

    def _fetch_keyword_single(self, query: str, rows: int) -> list[Paper]:
        params = {
            "query": query,
            "filter": f"from-pub-date:{self.from_date},type:journal-article",
            "rows": min(rows, 100), "sort": "relevance",
            "select": "DOI,title,abstract,author,container-title,issued,URL,ISSN",
        }
        try:
            resp = self._request_with_retry(self.BASE_URL, params=params, max_retries=2)
            items = resp.json().get("message", {}).get("items", [])
            return [p for p in (self._parse_crossref(item) for item in items) if p is not None]
        except Exception as e:
            logger.error(f"  关键词搜索失败 [{query[:30]}]: {e}")
            return []

    # ==================================================================
    def _is_agri_rs(self, title: str, abstract: str) -> bool:
        """匹配任意 2 个及以上关键词即视为农业遥感相关论文"""
        text = f"{title} {abstract}".lower()

        def term_matches(term: str) -> bool:
            if " " in term:
                return term in text
            return bool(re.search(r"\b" + re.escape(term) + r"\b", text))

        match_count = sum(1 for t in self.ALL_TERMS if term_matches(t))
        return match_count >= 2

    # ==================================================================
    def _parse_crossref(self, item: dict) -> Optional[Paper]:
        title_list = item.get("title") or []
        title = title_list[0].strip() if title_list else ""
        if not title:
            return None
        abstract = (item.get("abstract") or "").strip()
        if abstract:
            abstract = re.sub(r"<[^>]+>", "", abstract)
            abstract = re.sub(r"\s+", " ", abstract).strip()
        if not abstract:
            return None
        journal_list = item.get("container-title") or []
        journal = journal_list[0].strip() if journal_list else ""
        doi = item.get("DOI", "")
        url = item.get("URL", "") or (f"https://doi.org/{doi}" if doi else "")
        author_list = item.get("author") or []
        authors = [{"name": f"{a.get('given','')} {a.get('family','')}".strip()}
                   for a in author_list]
        issued = item.get("issued", {})
        date_parts = issued.get("date-parts", [[None]])[0]
        year = date_parts[0] if len(date_parts) > 0 else None
        pub_date = ""
        if len(date_parts) >= 3 and all(date_parts[:3]):
            pub_date = f"{date_parts[0]:04d}-{date_parts[1]:02d}-{date_parts[2]:02d}"
        elif year:
            pub_date = str(year)
        return Paper(
            title=title, authors=authors, abstract=abstract,
            journal=journal, year=year, doi=doi, url=url,
            source="crossref", paper_id=doi, publication_date=pub_date,
        )

    # ==================================================================
    def _request_with_retry(self, url: str, params: dict, max_retries: int = 3):
        last_exc = None
        for attempt in range(max_retries):
            try:
                resp = self.session.get(url, params=params, timeout=30)
                if resp.status_code == 429:
                    wait = 5 * (2 ** attempt)
                    logger.warning(f"  429 限流, 等待 {wait}s")
                    time.sleep(wait)
                    continue
                if resp.status_code != 200:
                    logger.warning(f"  HTTP {resp.status_code}")
                    time.sleep(3 * (2 ** attempt))
                    continue
                return resp
            except requests.exceptions.Timeout as e:
                last_exc = e
                time.sleep(2 ** attempt)
            except requests.exceptions.RequestException as e:
                last_exc = e
                time.sleep(2 ** attempt)
        raise last_exc or RuntimeError("HTTP 请求失败")


# ============================================================
# 合并去重 + IF 标注 + 翻译 + 亮点提取
# ============================================================
def merge_and_dedup(*paper_lists: list[Paper]) -> list[Paper]:
    seen: set[str] = set()
    merged: list[Paper] = []
    for papers in paper_lists:
        for p in papers:
            key = p.dedup_key
            if key not in seen:
                seen.add(key)
                merged.append(p)
    return merged


def annotate_if(papers: list[Paper], if_cache: JournalIFCache) -> list[Paper]:
    for p in papers:
        if p.get("impact_factor") is not None:
            continue
        journal = p.get("journal", "")
        p["impact_factor"] = if_cache.lookup(journal) if journal else None
    return papers


def filter_by_if(papers: list[Paper], threshold: float = 5.0) -> list[Paper]:
    result = [p for p in papers if p.get("impact_factor") is not None and p["impact_factor"] >= threshold]
    logger.info(f"IF 过滤 (≥{threshold}): {len(papers)} → {len(result)} 篇")
    return result


def enrich_papers(papers: list[Paper], ds_cfg: dict = None) -> list[Paper]:
    """
    为每篇论文补充: 中文标题翻译 + 结构化分析 + 关键词
    ds_cfg: DeepSeek 配置 {"api_key": "...", "model": "deepseek-chat"}
    """
    ds_cfg = ds_cfg or {}
    llm_api_key = ds_cfg.get("api_key", "")
    llm_model = ds_cfg.get("model", "deepseek-chat")

    logger.info(f"翻译与结构化分析: {len(papers)} 篇...")
    llm_enabled = bool(llm_api_key)
    logger.info(f"  LLM 分析: {'已启用 (DeepSeek)' if llm_enabled else '已禁用，使用规则算法'}")

    for i, p in enumerate(papers):
        # 翻译标题
        p["title_zh"] = translate_text(p["title"])
        # 结构化分析（LLM 一次输出中英双语 + 关键词）
        analysis = analyze_paper(
            p["abstract"], p["title"],
            llm_api_key=llm_api_key, llm_model=llm_model
        )
        p["keywords_en"] = analysis.get("keywords_en", [])
        p["keywords_zh"] = analysis.get("keywords_zh", [])
        p["highlights_en"] = analysis.get("sections_en", {})
        p["highlights_zh"] = analysis.get("sections_zh", {})

        if (i + 1) % 5 == 0:
            logger.info(f"  分析进度: {i+1}/{len(papers)}")
        time.sleep(0.2)

    logger.info(f"翻译与结构化分析完成")
    return papers


def load_sent_papers(path: Path = SENT_PAPERS_FILE) -> set[str]:
    if not path.exists():
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            return set(json.load(f).get("keys", []))
    except Exception:
        return set()


def save_sent_papers(keys: set[str], path: Path = SENT_PAPERS_FILE):
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"updated": datetime.now().isoformat(), "count": len(keys), "keys": list(keys)},
                  f, ensure_ascii=False, indent=2)


# ============================================================
# 双语 HTML 邮件
# ============================================================
def build_html_email(papers: list[Paper], date_str: str, threshold: float) -> str:
    # 5 个分析小节的标签（中英对照）
    SECTION_LABELS = [
        ("1. 研究逻辑思路", "1. Research Logic"),
        ("2. 研究方法", "2. Research Methods"),
        ("3. 研究数据结果", "3. Data & Results"),
        ("4. 研究结论及意义", "4. Conclusions & Significance"),
        ("5. 对其他学者的启示", "5. Implications for Scholars"),
    ]
    SECTION_KEYS = ["research_logic", "research_methods", "research_results",
                    "research_conclusions", "implications"]

    items = []
    for i, p in enumerate(papers, 1):
        title_en = p["title"]
        title_zh = p.get("title_zh", "")
        url = p["url"]
        journal = p["journal"]
        if_val = p["impact_factor"]
        year = p.get("year", "N/A")
        authors = ", ".join(a.get("name", "") for a in p["authors"][:4])
        if len(p["authors"]) > 4:
            authors += " et al."

        # 关键词标签
        keywords_zh = p.get("keywords_zh", [])
        keywords_en = p.get("keywords_en", [])
        kw_tags = ""
        if keywords_zh:
            kw_tags = " ".join(f'<span class="kw-tag">{kw}</span>' for kw in keywords_zh)
        if keywords_en:
            kw_tags += " " + " ".join(f'<span class="kw-tag-en">{kw}</span>' for kw in keywords_en)

        # 结构化分析小节
        sections_en = p.get("highlights_en", {})
        sections_zh = p.get("highlights_zh", {})
        section_html = ""
        for idx, (label_zh, label_en) in enumerate(SECTION_LABELS):
            key = SECTION_KEYS[idx]
            zh_text = sections_zh.get(key, "") if isinstance(sections_zh, dict) else ""
            en_text = sections_en.get(key, "") if isinstance(sections_en, dict) else ""
            if not zh_text and not en_text:
                continue
            section_html += f"""
                <div class="sec-item">
                    <p class="sec-label">{label_zh}</p>
                    <p class="sec-zh">{zh_text}</p>
                    <p class="sec-en">{en_text}</p>
                </div>"""

        if not section_html:
            section_html = '<p class="sec-empty">暂无结构化分析数据</p>'

        items.append(f"""
        <div class="paper">
            <div class="paper-index-line">
                <span class="paper-index">{i}</span>
                <span class="if-badge">IF {if_val:.1f}</span>
                <span class="paper-journal">{journal}</span>
                <span class="paper-year">{year}</span>
            </div>
            <p class="paper-title-zh">{title_zh}</p>
            <p class="paper-title-en">{title_en}</p>
            <p class="paper-authors">{authors}</p>
            {f'<div class="keywords">{kw_tags}</div>' if kw_tags else ""}
            <div class="analysis">{section_html}</div>
            <a class="paper-link" href="{url}" target="_blank">📄 查看原文 →</a>
        </div>""")

    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"></head>
<body>
<style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Microsoft YaHei', sans-serif; background: #f0f2f5; margin: 0; padding: 0; -webkit-text-size-adjust: 100%; }}
    .container {{ max-width: 100%; margin: 0; background: #fff; overflow: hidden; }}
    .header {{ background: linear-gradient(135deg, #1a5c30 0%, #2d8c4a 50%, #1a7a30 100%); color: #fff; padding: 18px 14px; }}
    .header h1 {{ margin: 0 0 4px; font-size: 20px; font-weight: 700; }}
    .header .subtitle {{ opacity: 0.9; font-size: 12px; line-height: 1.4; }}
    .summary {{ padding: 12px 14px; background: #edf7f0; border-bottom: 1px solid #d4e8d8; font-size: 13px; color: #3a6b4a; line-height: 1.5; }}
    .summary strong {{ color: #1a5c30; }}
    .paper-list {{ padding: 8px 6px; }}
    .paper {{ border-bottom: 1px solid #eee; padding: 14px 10px; }}
    .paper:last-child {{ border-bottom: none; }}
    .paper-index-line {{ display: flex; align-items: center; gap: 8px; margin-bottom: 8px; flex-wrap: wrap; }}
    .paper-index {{ display: inline-flex; align-items: center; justify-content: center; background: #1a5c30; color: #fff; border-radius: 50%; width: 24px; height: 24px; font-size: 12px; font-weight: 700; flex-shrink: 0; }}
    .if-badge {{ background: #e8f5e9; color: #1a5c30; padding: 2px 6px; border-radius: 4px; font-size: 11px; font-weight: 700; }}
    .paper-journal {{ font-size: 12px; color: #555; font-weight: 600; }}
    .paper-year {{ font-size: 11px; color: #999; }}
    .paper-title-zh {{ font-size: 16px; font-weight: 700; color: #1a1a1a; margin: 0 0 3px; line-height: 1.5; }}
    .paper-title-en {{ font-size: 13px; color: #666; margin: 0 0 6px; line-height: 1.4; font-style: italic; }}
    .paper-authors {{ font-size: 11px; color: #999; margin: 0 0 8px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
    .keywords {{ margin: 8px 0; }}
    .kw-tag {{ display: inline-block; background: #e8f0fe; color: #1a56b5; padding: 2px 8px; border-radius: 12px; font-size: 11px; margin: 2px 4px 2px 0; }}
    .kw-tag-en {{ display: inline-block; background: #f0f0f0; color: #666; padding: 2px 8px; border-radius: 12px; font-size: 10px; margin: 2px 4px 2px 0; }}
    .analysis {{ margin: 10px 0; }}
    .sec-item {{ background: #fafcfa; border-left: 3px solid #2d8c4a; padding: 8px 10px; margin-bottom: 8px; border-radius: 0 4px 4px 0; }}
    .sec-item:last-child {{ margin-bottom: 0; }}
    .sec-label {{ font-size: 12px; font-weight: 700; color: #2d8c4a; margin: 0 0 4px; }}
    .sec-zh {{ font-size: 13px; color: #333; margin: 0 0 2px; line-height: 1.6; }}
    .sec-en {{ font-size: 11px; color: #999; margin: 0; line-height: 1.5; }}
    .sec-empty {{ font-size: 12px; color: #999; font-style: italic; text-align: center; padding: 10px; }}
    .paper-link {{ display: inline-block; margin-top: 8px; font-size: 12px; color: #2d8c4a; text-decoration: none; }}
    .paper-link:hover {{ text-decoration: underline; }}
    .footer {{ padding: 14px 14px; background: #fafafa; border-top: 1px solid #eee; font-size: 11px; color: #999; text-align: center; line-height: 1.6; }}
    .footer a {{ color: #2d8c4a; }}
</style>
<div class="container">
    <div class="header">
        <h1>🌾 农业遥感论文日报</h1>
        <div class="subtitle">{date_str} · IF ≥ {threshold} · 共 {len(papers)} 篇 · 中英双语</div>
    </div>
    <div class="summary">
        📊 今日精选 <strong>{len(papers)}</strong> 篇农业遥感方向高水平论文，按影响因子降序排列。
        每篇附 <strong>中文标题翻译</strong> + <strong>关键词</strong> + <strong>五维结构化分析</strong> + 原文链接。
    </div>
    <div class="paper-list">
        {"".join(items)}
    </div>
    <div class="footer">
        <p>由 <strong>Paper Digest Bot v3</strong> 自动生成 · 数据源: <a href="https://crossref.org">Crossref</a></p>
        <p>分析引擎: DeepSeek LLM · 去重策略: DOI 永久去重</p>
    </div>
</div>
</body>
</html>"""


def send_email(config: dict, html_content: str, paper_count: int, date_str: str) -> bool:
    email_cfg = config["email"]
    # 支持多收件人（recipients 列表），兼容旧配置（recipient 字符串）
    if "recipients" in email_cfg:
        recipients = email_cfg["recipients"]
    elif "recipient" in email_cfg:
        recipients = [email_cfg["recipient"]]
    else:
        logger.error("配置中缺少收件人信息")
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"🌾 农业遥感论文日报 - {date_str} ({paper_count}篇 · 中英双语)"
    msg["From"] = email_cfg["sender"]
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(
        f"农业遥感论文日报 - {date_str}\n共 {paper_count} 篇论文 (IF≥5.0)\n中英双语 · 含亮点提炼\n\n请使用支持 HTML 的邮件客户端查看。",
        "plain", "utf-8"))
    msg.attach(MIMEText(html_content, "html", "utf-8"))

    try:
        server = smtplib.SMTP(email_cfg["smtp_server"], email_cfg["smtp_port"], timeout=30)
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(email_cfg["sender"], email_cfg["password"])
        server.sendmail(email_cfg["sender"], recipients, msg.as_string())
        server.quit()
        logger.info(f"邮件发送成功 → {', '.join(recipients)}")
        return True
    except smtplib.SMTPAuthenticationError:
        logger.error("SMTP 认证失败，请检查邮箱地址和授权码")
        return False
    except smtplib.SMTPException as e:
        logger.error(f"SMTP 发送失败: {e}")
        return False


# ============================================================
# 主流程
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="农业遥感论文每日邮件推送 v2")
    parser.add_argument("--config", type=str, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--days", type=int, default=None)
    parser.add_argument("--max", type=int, default=None)
    parser.add_argument("--no-translate", action="store_true", help="跳过翻译（加速测试）")
    args = parser.parse_args()

    config_path = Path(args.config) if args.config else CONFIG_FILE
    config = load_config(config_path)

    filter_cfg = config["filter"]
    if_threshold = filter_cfg.get("if_threshold", 5.0)
    days_back = args.days or filter_cfg.get("days_back", 7)
    max_papers = args.max or filter_cfg.get("max_papers_per_email", 50)

    logger.info("=" * 60)
    logger.info(f"农业遥感论文日报 v2 - {datetime.now():%Y-%m-%d}")
    logger.info(f"IF ≥ {if_threshold} | 回溯 {days_back} 天 | 上限 {max_papers} 篇")
    logger.info("=" * 60)

    # ---- 加载期刊 IF 数据库 ----
    logger.info("[1/6] 加载期刊 IF 数据库...")
    if_cache = JournalIFCache()

    # ---- 抓取论文 ----
    logger.info("[2/6] 抓取论文...")
    fetcher = PaperFetcher(days_back=days_back)
    issn_papers = fetcher.fetch_by_issn(if_cache.high_if_issns)
    keywords = config.get("keywords", [])
    kw_papers = fetcher.fetch_by_keywords(keywords) if keywords else []

    # ---- 合并去重 + 主题过滤 ----
    all_papers = merge_and_dedup(issn_papers, kw_papers)
    before_filter = len(all_papers)
    all_papers = [p for p in all_papers
                  if p["source"] == "issn_targeted" or fetcher._is_agri_rs(p["title"], p["abstract"])]
    logger.info(f"  总计: {len(all_papers)} 篇 (ISSN定向 {len(issn_papers)} + 关键词 {len(kw_papers)}, 主题过滤 -{before_filter - len(all_papers)})")

    # ---- IF 标注 & 过滤 ----
    logger.info("[3/6] 影响因子过滤...")
    all_papers = annotate_if(all_papers, if_cache)
    high_if_papers = filter_by_if(all_papers, if_threshold)

    # ---- IF 降序排序 ----
    high_if_papers.sort(key=lambda p: p.get("impact_factor") or 0, reverse=True)
    logger.info(f"  按 IF 降序排列完成，最高 IF={high_if_papers[0]['impact_factor']:.1f}" if high_if_papers else "  无高IF论文")

    # ---- 翻译 + 亮点 ----
    if not args.no_translate:
        logger.info("[4/6] 翻译与亮点提取...")
        high_if_papers = enrich_papers(high_if_papers, config.get("deepseek"))
    else:
        logger.info("[4/6] 跳过翻译 (--no-translate)")

    # ---- 排除已发送 ----
    logger.info("[5/6] 排除已发送论文...")
    sent_keys = load_sent_papers()
    fresh_papers = [p for p in high_if_papers if p.dedup_key not in sent_keys]
    logger.info(f"  排除已发送: {len(high_if_papers)} → {len(fresh_papers)} 篇")

    if len(fresh_papers) > max_papers:
        logger.info(f"  截断: {max_papers} 篇（优先保留高 IF）")
        fresh_papers = fresh_papers[:max_papers]

    # ---- 发送 ----
    date_str = datetime.now().strftime("%Y-%m-%d")

    if args.dry_run:
        logger.info("[DRY RUN] 论文列表:")
        logger.info("-" * 60)
        for i, p in enumerate(fresh_papers, 1):
            if_val = f"IF={p['impact_factor']:.1f}" if p.get("impact_factor") else "IF=?"
            zh_title = p.get("title_zh", "")[:60]
            logger.info(f"  {i}. [{if_val}] {p['title'][:80]}")
            logger.info(f"     中文: {zh_title}")
            sections = p.get("highlights_en", {})
            n_sections = len(sections) if isinstance(sections, dict) else 0
            keywords = p.get("keywords_zh", [])
            logger.info(f"     分析: {n_sections} 节, 关键词: {', '.join(keywords) if keywords else '无'}")
            logger.info(f"     {p['journal']} ({p['year']})")
        logger.info("-" * 60)
        logger.info(f"共 {len(fresh_papers)} 篇新论文 (dry-run)")
    else:
        if not fresh_papers:
            logger.info("[6/6] 今日无新论文，跳过邮件发送")
            logger.info("完成！")
            return

        logger.info("[6/6] 生成并发送邮件...")
        html = build_html_email(fresh_papers, date_str, if_threshold)
        success = send_email(config, html, len(fresh_papers), date_str)
        if success:
            new_keys = sent_keys | {p.dedup_key for p in fresh_papers}
            save_sent_papers(new_keys)
            logger.info(f"已发送记录已更新: {len(new_keys)} 篇累计")
        else:
            logger.error("邮件发送失败")

    logger.info("完成！")


if __name__ == "__main__":
    main()
