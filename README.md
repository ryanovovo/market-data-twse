# TWSE API 抓取範例（`uv`）

這個專案提供以下資料抓取：
- 股價歷史（日線）
- 還原股價（前復權）
- 融資融券
- 當沖交易資訊
- 每日周轉率（以成交股數計算）
- 信用額度總量管制餘額表（融券/借券賣出餘額）
- 三大法人買賣超（外資/投信/自營商）
- 可借券賣出股數（借券可用量快照）
- 所有上市公司清單

## 1. 使用 `uv` 建立與同步環境

```bash
uv sync
```

## 2. 指令用法

```bash
uv run main.py [--delay 1.0] [--log-level INFO] [--output-format auto|csv|parquet] <command> --stock <股票代號> --start <YYYY-MM-DD> [--end <YYYY-MM-DD>] --out <輸出路徑>
```

可選節流參數（建議批次抓取時加大）：

```bash
uv run main.py <command> ... --delay 1.0
```

- 預設 `--delay 1.0` 秒
- `--end` 可省略，會自動使用最近一個交易日
- `--output-format` 預設 `auto`：依 `--out` 副檔名判斷；若 `--out` 無副檔名則預設 `csv`
- 使用 `loguru` 輸出執行紀錄（可用 `--log-level` 調整層級）
- 內建隨機抖動與重試退避（遇到 429/5xx 會自動慢下來）
- 日資料查詢會自動跳過週末，減少無效請求
- 長時間任務會顯示 `tqdm` 進度條
- 若 `openapi.twse.com.tw` 無法連線，上市公司清單會自動 fallback 到專案內建 `listed_companies_snapshot.csv`

### 股價（日線）

```bash
uv run main.py price --stock 2330 --start 2026-01-01 --end 2026-03-06 --out data/2330_price.csv
```

### 還原股價（前復權）

```bash
uv run main.py adjusted-price --stock 2330 --start 2024-01-01 --end 2026-03-06 --out data/2330_adjusted_price.csv
```

### 融資融券

```bash
uv run main.py margin --stock 2330 --start 2026-02-20 --end 2026-03-06 --out data/2330_margin.csv
```

### 當沖交易資訊

```bash
uv run main.py daytrade --stock 2330 --start 2026-02-20 --end 2026-03-06 --out data/2330_daytrade.csv
```

### 信用額度總量管制餘額表（融券 / 借券賣出餘額）

```bash
uv run main.py credit-quota --stock 2330 --start 2026-02-20 --end 2026-03-06 --out data/2330_credit_quota.csv
```

### 可借券賣出股數（借券可用量）

```bash
uv run main.py sbl --stock 2330 --out data/2330_sbl_available.csv
```

### 所有上市公司清單

```bash
uv run main.py listed-companies --out data/listed_companies.csv
```

### 單日全市場（上市公司）整合資料

```bash
uv run main.py market-day-all --date 2026-03-06 --out data/market_day_all_2026-03-06.csv
```

### 區間全市場（上市公司）整合資料（自動略過已下載）

```bash
uv run main.py market-range-all --start 2018-01-01 --end 2025-12-31
```

輸出 parquet 範例：

```bash
uv run main.py --output-format parquet market-range-all --start 2018-01-01 --end 2025-12-31
```

- 每個交易日輸出一個檔案：`data/market_day_all_YYYY-MM-DD.csv`
- `--out-dir` 預設為 `data`
- 若該日檔案已存在，會自動跳過不重抓
- 下載後會自動回補既有檔案：`adj_*`（`TWT49U`）、`industry_code`、`next_*daytrade`、`inst_*`（三大法人）
- 如需覆蓋重抓可加 `--overwrite`
- 如需略過回補可加 `--skip-adj-backfill`

### 一次整合全部到單一檔案（CSV / Parquet）

```bash
uv run main.py all-in-one --stock 2330 --start 2026-02-20 --end 2026-03-06 --out data/2330_all_in_one.csv
```

輸出 parquet 範例：

```bash
uv run main.py all-in-one --stock 2330 --start 2026-02-20 --end 2026-03-06 --out data/2330_all_in_one.parquet
```

可選：自行指定流通股數（不使用 API 回傳的已發行股數）

```bash
uv run main.py all-in-one --stock 2330 --start 2026-02-20 --end 2026-03-06 --shares 25930380000 --out data/2330_all_in_one.csv
```

### 每日周轉率

```bash
uv run main.py turnover --stock 2330 --start 2026-02-20 --end 2026-03-06 --out data/2330_turnover_daily.csv
```

可選：自行指定流通股數（不使用 API 回傳的已發行股數）

```bash
uv run main.py turnover --stock 2330 --start 2026-02-20 --end 2026-03-06 --shares 25930380000 --out data/2330_turnover_daily.csv
```

## 3. 周轉率計算方式

`每日周轉率(%) = 當日成交股數 / 已發行普通股數 × 100`

- 當日成交股數：`STOCK_DAY`
- 已發行普通股數：優先使用 `MI_QFIIS` 的當日 `發行股數`
- 若該日查無 `MI_QFIIS`，回退使用 `t187ap03_L`（或你傳入的 `--shares`）

## 3-1. 還原股價計算方式（前復權）

- 事件來源：`TWT49U` 的「除權息前收盤價」與「除權息參考價」
- 單一事件因子：`factor = 除權息參考價 / 除權息前收盤價`
- 每日還原係數：該日之後所有事件因子連乘（`adj_factor_back`）
- `adj_open/adj_high/adj_low/adj_close = 原始價 × adj_factor_back`

## 4. 使用到的證交所端點

- 股價日線：`https://www.twse.com.tw/exchangeReport/STOCK_DAY`
- 除權除息計算結果（歷史）：`https://www.twse.com.tw/rwd/zh/exRight/TWT49U`
- 股價升降幅度（含次一交易日開盤參考價/漲跌停）：`https://www.twse.com.tw/rwd/zh/variation/TWT84U`
- 融資融券：`https://www.twse.com.tw/exchangeReport/MI_MARGN`
- 當沖交易：`https://www.twse.com.tw/exchangeReport/TWTB4U`
- 信用額度總量管制餘額表：`https://www.twse.com.tw/exchangeReport/TWT93U`
- 三大法人買賣超：`https://www.twse.com.tw/rwd/zh/fund/T86`
- 上市可借券賣出股數：`https://openapi.twse.com.tw/v1/SBL/TWT96U`
- 每日發行股數：`https://www.twse.com.tw/fund/MI_QFIIS`
- 公司基本資料（取股數）：`https://openapi.twse.com.tw/v1/opendata/t187ap03_L`

## 5. `all-in-one` 內容

每一列是一個交易日，會包含：
- 股價（STOCK_DAY）
- 產業別（`industry_code`）
- 還原股價（`adj_factor_back`、`adj_open`、`adj_high`、`adj_low`、`adj_close`）
- 次一交易日價位（`next_open_ref_price`、`next_limit_up_price`、`next_limit_down_price`）
- 次一交易日當沖狀態（`next_can_daytrade`、`next_daytrade_type`、`next_daytrade_suspension_flag`）
- 融資融券（MI_MARGN）
- 當沖（TWTB4U，含 `can_daytrade`、`daytrade_type`）
- 信用額度總量管制 / 借券賣出餘額（TWT93U）
- 三大法人買賣超（T86，`inst_*` 欄位）
- 日周轉率（STOCK_DAY + MI_QFIIS / t187ap03_L）
- 最新可借券賣出股數快照（TWT96U，欄位 `sbl_available_volume_latest`）

次一交易日價位來源：`/rwd/zh/variation/TWT84U`，以其 `最近成交日` 對應回當日資料。
