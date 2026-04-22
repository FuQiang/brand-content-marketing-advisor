# 飞书多维表格写入形态速查

BCMA 使用的 6 种字段类型,写入时的 JSON 形态必须精确匹配,否则会触发 1254066/1254064/1254015 等错误.

官方完整指南见 `/home/admin/.openclaw/extensions/openclaw-lark/skills/feishu-bitable/references/field-value-shapes.md`.

## 文本 (Text)

```json
"fields": {
  "品牌名称": "双汇"
}
```

纯字符串.不能是 null / 数组.

## 数字 (Number)

```json
"fields": {
  "总分": 17,
  "R4 舆情风险": 5,
  "入库时间": 1745308800000
}
```

时间戳字段(`入库时间`/`抓取时间`/`生成时间`)统一毫秒,不要传字符串.

## 单选 (SingleSelect)

```json
"fields": {
  "产品生命周期": "新品",
  "决策结果": "✅ 主推"
}
```

值必须**字符串**,且必须是该字段已有选项.若传入新值且该字段允许"编辑时新增",会自动补;不允许则报 1254064.

## 多选 (MultiSelect)

```json
"fields": {
  "典型人群受众": ["精致妈妈"],
  "目标人群标签": ["新锐白领", "精致妈妈"],
  "适用平台": ["抖音", "小红书"]
}
```

即便只有一个值也必须是**数组**.写成字符串会报 1254064.

## URL (Hyperlink)

```json
"fields": {
  "官方来源链接": {"link": "https://www.example.com", "text": "官网"}
}
```

或简化形式(飞书会自动补 text):
```json
"fields": {
  "官方来源链接": {"link": "https://www.example.com"}
}
```

不支持纯字符串 URL —— 会落成文本而非超链接.

## 附件 (Attachment)

分两步:
1. 先 `feishu_drive_file.upload` 传文件到 Drive,返回 `file_token`
2. 把 file_token 数组写入附件字段

```json
"fields": {
  "产品图库(真实大片)": [
    {"file_token": "boxb4Xxxxxxxxxxxxx"},
    {"file_token": "boxb4Yyyyyyyyyyyy"}
  ]
}
```

`file_token` 必须通过 `feishu_drive_file.upload` 取得,并且该 token 对应的文件必须在 Bitable 所在的父节点(parent_node=`app_token`, parent_type="bitable_file").否则附件展示为"已失效".

## 人员 (User)

```json
"fields": {
  "负责人": [{"id": "ou_xxxxxxxxxxxxx"}]
}
```

用 open_id.BCMA 暂不用这类字段,但跨表时可能遇到.

## 日期 (DateTime)

```json
"fields": {
  "上市年月": 1696118400000
}
```

毫秒时间戳.BCMA `上市年月` 字段定义为**文本**(便于 YYYY / YYYY-MM 这种模糊格式),所以实际写文本形态.

## 常见错误码

| 错误码 | 原因 | 修法 |
|---|---|---|
| 1254045 | FieldNameNotFound | 字段不存在 → 调 `feishu_bitable_app_table_field.create` 补字段后重试,字段名从 config.yaml 的 fields 映射取 |
| 1254043 | 字段类型不匹配 | 查本文件对照字段类型,改写入形态后重试 |
| 1254064 | 单选/多选值不合法 | 枚举校验:检查是否拼写错误或缺少选项 |
| 1254066 | 附件 file_token 无效 | 重新走 `feishu_drive_file.upload` |
| 1254015 | 多选传了字符串 | 包一层数组 |
| 1254019 | 字段值太长 | 文本字段默认 4 万字上限,超了要截断 |

## 批量写入

多条同表记录优先用 `feishu_bitable_app_table_record.batch_create`(单次最多 500 条);BCMA 的 Step 2 人群(1-3 条)、Step 3 产品(5-10 条)、Step 5 话题(≤5 条)都可批量.

附件字段每条批量写入仍受"file_token 需 upload 取得"约束,所以 Step 3 图库、Step 6 封面/视频只能单条 create / update.
