# Schema 說明

此檔案說明 `main.py` 各指令輸出 CSV 的欄位意義。

## 通用規則

- 日期格式：`YYYY-MM-DD`
- 支援輸出格式：`CSV`（`UTF-8-SIG`）或 `Parquet`
- 數值欄位多以字串輸出（保留原始資料樣式，空字串代表無資料）
- `turnover_ratio_pct`、`adj_*` 為程式計算欄位
- 逐日抓取流程使用 `exchange-calendars` 的 `XTAI` 交易日曆（非單純週一到週五）

## 1) `price`（個股日線）

| 欄位 | 說明 |
|---|---|
| `date` | 交易日 |
| `stock_no` | 股票代號 |
| `volume` | 成交股數 |
| `value` | 成交金額 |
| `open` | 開盤價 |
| `high` | 最高價 |
| `low` | 最低價 |
| `close` | 收盤價 |
| `change` | 漲跌價差 |
| `transactions` | 成交筆數 |
| `note` | 註記（如除權息等，依 TWSE 原始欄位） |

## 2) `adjusted-price`（個股前復權）

包含 `price` 的全部欄位，再加下列欄位：

| 欄位 | 說明 |
|---|---|
| `adj_factor_back` | 前復權係數（該日之後事件因子連乘） |
| `adj_open` | 還原開盤價 = `open * adj_factor_back` |
| `adj_high` | 還原最高價 = `high * adj_factor_back` |
| `adj_low` | 還原最低價 = `low * adj_factor_back` |
| `adj_close` | 還原收盤價 = `close * adj_factor_back` |

計算來源：`TWT49U`（除權息前收盤價、除權息參考價）。

## 3) `margin`（融資融券）

| 欄位 | 說明 |
|---|---|
| `date` | 交易日 |
| `stock_no` | 股票代號 |
| `stock_name` | 股票名稱 |
| `margin_buy` | 融資買進 |
| `margin_sell` | 融資賣出 |
| `margin_cash_repayment` | 融資現金償還 |
| `margin_prev_balance` | 融資前日餘額 |
| `margin_balance` | 融資今日餘額 |
| `short_buy` | 融券買進 |
| `short_sell` | 融券賣出 |
| `short_cash_repayment` | 融券現券償還 |
| `short_prev_balance` | 融券前日餘額 |
| `short_balance` | 融券今日餘額 |
| `offset` | 資券互抵 |
| `note` | 備註 |

## 4) `daytrade`（當沖）

| 欄位 | 說明 |
|---|---|
| `date` | 交易日 |
| `stock_no` | 股票代號 |
| `stock_name` | 股票名稱 |
| `can_daytrade` | 是否可當沖（`Y`/`N`） |
| `daytrade_type` | 當沖種類（`雙向皆可` / `僅先買後賣` / `不可當沖`） |
| `suspension_flag` | `Y` 代表僅先買後賣，空白/其他通常為雙向 |
| `daytrade_volume` | 當沖成交股數 |
| `daytrade_buy_value` | 當沖買進成交金額 |
| `daytrade_sell_value` | 當沖賣出成交金額 |

## 5) `credit-quota`（信用額度總量管制餘額）

| 欄位 | 說明 |
|---|---|
| `date` | 交易日 |
| `stock_no` | 股票代號 |
| `stock_name` | 股票名稱 |
| `margin_short_prev_balance` | 融券前日餘額 |
| `margin_short_sell` | 融券賣出 |
| `margin_short_buy` | 融券買進 |
| `margin_short_cash_repayment` | 融券現券償還 |
| `margin_short_balance` | 融券當日餘額 |
| `margin_short_next_limit` | 次一營業日融券賣出可限額 |
| `sbl_prev_balance` | 借券賣出前日餘額 |
| `sbl_sell` | 借券賣出 |
| `sbl_return` | 借券還券 |
| `sbl_adjust` | 借券調整 |
| `sbl_balance` | 借券賣出當日餘額 |
| `sbl_next_limit` | 次一營業日借券賣出可限額 |
| `note` | 備註（符號請參照「11) 信用交易備註符號（TWT93U）」） |

## 6) `sbl`（可借券賣出股數快照）

| 欄位 | 說明 |
|---|---|
| `stock_no` | 股票代號 |
| `sbl_available_volume` | 可借券賣出股數（快照值） |

## 7) `listed-companies`（上市公司清單）

| 欄位 | 說明 |
|---|---|
| `report_date` | 出表日期 |
| `stock_no` | 公司代號 |
| `company_name` | 公司名稱 |
| `short_name` | 公司簡稱 |
| `industry_code` | 產業別 |
| `listing_date` | 上市日期 |
| `issued_shares` | 已發行普通股數或 TDR 原股發行股數 |
| `paid_in_capital` | 實收資本額 |
| `chairman` | 董事長 |
| `general_manager` | 總經理 |
| `spokesperson` | 發言人 |
| `phone` | 總機電話 |
| `address` | 公司地址 |
| `website` | 公司網址 |

## 8) `turnover`（每日周轉率）

| 欄位 | 說明 |
|---|---|
| `date` | 交易日 |
| `stock_no` | 股票代號 |
| `volume` | 成交股數 |
| `issued_shares` | 計算使用的已發行股數 |
| `issued_shares_source` | 股數來源：`MI_QFIIS` 或 `t187ap03_L/--shares` |
| `turnover_ratio_pct` | 周轉率(%) = `volume / issued_shares * 100` |

## 9) `market-day-all` / `market-range-all`（全市場整合）

`market-range-all` 會輸出多個檔案 `market_day_all_YYYY-MM-DD.csv`，每個檔案欄位相同。

### 9.1 完整欄位 Schema（依輸出順序，逐欄詳細說明）

| 欄位 | 型別 | 詳細說明 | 來源（官方欄位） |
|---|---|---|---|
| `date` | `date string` | 交易日期（西元 `YYYY-MM-DD`）。此值等於查詢日。 | `MI_INDEX` 查詢參數 `date` |
| `stock_no` | `string` | 證券代號。 | `MI_INDEX` 欄位「證券代號」 |
| `stock_name` | `string` | 證券名稱。 | `MI_INDEX` 欄位「證券名稱」 |
| `industry_code` | `string` | 產業別代碼（證交所上市公司分類代碼）。 | `t187ap03_L` 欄位「產業別」 |
| `price_volume` | `numeric string` | 當日成交股數（股）。 | `MI_INDEX` 欄位「成交股數」 |
| `price_value` | `numeric string` | 當日成交金額（元）。 | `MI_INDEX` 欄位「成交金額」 |
| `price_open` | `numeric string` | 開盤價（元）。 | `MI_INDEX` 欄位「開盤價」 |
| `price_high` | `numeric string` | 最高價（元）。 | `MI_INDEX` 欄位「最高價」 |
| `price_low` | `numeric string` | 最低價（元）。 | `MI_INDEX` 欄位「最低價」 |
| `price_close` | `numeric string` | 收盤價（元）。 | `MI_INDEX` 欄位「收盤價」 |
| `price_change` | `signed numeric string` | 漲跌價差（元，含正負號）。程式把 `漲跌(+/-)` 與 `漲跌價差` 合併為單欄位。 | `MI_INDEX` 欄位「漲跌(+/-)」「漲跌價差」 |
| `price_transactions` | `numeric string` | 成交筆數（筆）。 | `MI_INDEX` 欄位「成交筆數」 |
| `adj_factor_back` | `decimal string` | 前復權係數。下載當下先寫 `1.000000000000`；`market-range-all` 結束後用除權息資料回補，定義為「該日之後事件因子連乘」。 | 程式計算（來源：`TWT49U`） |
| `adj_open` | `decimal string` | 還原開盤價（元）=`price_open * adj_factor_back`。 | 程式計算 |
| `adj_high` | `decimal string` | 還原最高價（元）=`price_high * adj_factor_back`。 | 程式計算 |
| `adj_low` | `decimal string` | 還原最低價（元）=`price_low * adj_factor_back`。 | 程式計算 |
| `adj_close` | `decimal string` | 還原收盤價（元）=`price_close * adj_factor_back`。 | 程式計算 |
| `next_trade_date` | `date string` | 次一交易日日期。只在 `TWT84U` 的「最近成交日」等於本列 `date` 時填入。 | `TWT84U` 欄位「最近成交日」「資料日期」 |
| `next_open_ref_price` | `numeric string` | 次一交易日開盤競價基準（元，俗稱開盤參考價）。 | `TWT84U` 欄位「開盤競價基準」（本日欄） |
| `next_limit_up_price` | `numeric string` | 次一交易日漲停價（元）。 | `TWT84U` 欄位「漲停價」 |
| `next_limit_down_price` | `numeric string` | 次一交易日跌停價（元）。 | `TWT84U` 欄位「跌停價」 |
| `next_can_daytrade` | `enum string` | 次一交易日是否可當沖（`Y`/`N`）。 | 程式推導（`TWTB4U`） |
| `next_daytrade_type` | `enum string` | 次一交易日當沖型態（`雙向皆可` / `僅先買後賣` / `不可當沖`）。 | 程式推導（`TWTB4U`） |
| `next_daytrade_suspension_flag` | `string` | 次一交易日當沖暫停現股賣出後現款買進註記（`Y` 常代表僅先買後賣）。 | `TWTB4U` 欄位「暫停現股賣出後現款買進當沖註記」 |
| `margn_margin_buy` | `numeric string` | 融資買進（股）。 | `MI_MARGN` 融資融券彙總欄位「買進」（融資） |
| `margn_margin_sell` | `numeric string` | 融資賣出（股）。 | `MI_MARGN` 欄位「賣出」（融資） |
| `margn_margin_cash_repayment` | `numeric string` | 融資現金償還（股）。 | `MI_MARGN` 欄位「現金償還」（融資） |
| `margn_margin_prev_balance` | `numeric string` | 融資前日餘額（股）。 | `MI_MARGN` 欄位「前日餘額」（融資） |
| `margn_margin_balance` | `numeric string` | 融資今日餘額（股）。 | `MI_MARGN` 欄位「今日餘額」（融資） |
| `margn_short_buy` | `numeric string` | 融券買進（股）。 | `MI_MARGN` 欄位「買進」（融券） |
| `margn_short_sell` | `numeric string` | 融券賣出（股）。 | `MI_MARGN` 欄位「賣出」（融券） |
| `margn_short_cash_repayment` | `numeric string` | 融券現券償還（股）。 | `MI_MARGN` 欄位「現券償還」（融券） |
| `margn_short_prev_balance` | `numeric string` | 融券前日餘額（股）。 | `MI_MARGN` 欄位「前日餘額」（融券） |
| `margn_short_balance` | `numeric string` | 融券今日餘額（股）。 | `MI_MARGN` 欄位「今日餘額」（融券） |
| `margn_offset` | `numeric string` | 資券互抵（股）。 | `MI_MARGN` 欄位「資券互抵」 |
| `margn_note` | `string` | 融資融券註記。 | `MI_MARGN` 欄位「註記」 |
| `can_daytrade` | `enum string` | 是否可當沖。程式規則：該日該代號存在於 `TWTB4U` -> `Y`；否則 `N`。 | 程式推導（依 `TWTB4U` 是否有資料） |
| `daytrade_type` | `enum string` | 當沖型態。程式規則：`daytrade_suspension_flag=Y` -> `僅先買後賣`；有資料且非 Y -> `雙向皆可`；無資料 -> `不可當沖`。 | 程式推導（依 `TWTB4U` 註記欄） |
| `daytrade_suspension_flag` | `string` | 暫停現股賣出後現款買進當沖註記。`Y` 時代表受限（僅先買後賣）。 | `TWTB4U` 欄位「暫停現股賣出後現款買進當沖註記」 |
| `daytrade_volume` | `numeric string` | 當日沖銷交易成交股數（股）。 | `TWTB4U` 欄位「當日沖銷交易成交股數」 |
| `daytrade_buy_value` | `numeric string` | 當日沖銷交易買進成交金額（元）。 | `TWTB4U` 欄位「當日沖銷交易買進成交金額」 |
| `daytrade_sell_value` | `numeric string` | 當日沖銷交易賣出成交金額（元）。 | `TWTB4U` 欄位「當日沖銷交易賣出成交金額」 |
| `quota_margin_short_prev_balance` | `numeric string` | 融券前日餘額（股）。 | `TWT93U` 欄位「前日餘額」（融券群組） |
| `quota_margin_short_sell` | `numeric string` | 融券賣出（股）。 | `TWT93U` 欄位「賣出」（融券群組） |
| `quota_margin_short_buy` | `numeric string` | 融券買進（股）。 | `TWT93U` 欄位「買進」（融券群組） |
| `quota_margin_short_cash_repayment` | `numeric string` | 融券現券償還（股）。 | `TWT93U` 欄位「現券」（融券群組） |
| `quota_margin_short_balance` | `numeric string` | 融券今日餘額（股）。 | `TWT93U` 欄位「今日餘額」（融券群組） |
| `quota_margin_short_next_limit` | `numeric string` | 次一營業日融券限額（股）。法規上當「融券賣出餘額 + 借券賣出餘額」達流通在外股數 25% 時，會啟動暫停賣出機制（詳第 13 節）。 | `TWT93U` 欄位「次一營業日限額」（融券群組） |
| `quota_sbl_prev_balance` | `numeric string` | 借券賣出前日餘額（股）。 | `TWT93U` 欄位「前日餘額」（借券賣出群組） |
| `quota_sbl_sell` | `numeric string` | 借券賣出（股）。 | `TWT93U` 欄位「當日賣出」 |
| `quota_sbl_return` | `numeric string` | 借券還券（股）。 | `TWT93U` 欄位「當日還券」 |
| `quota_sbl_adjust` | `numeric string` | 借券當日調整（股）。 | `TWT93U` 欄位「當日調整」 |
| `quota_sbl_balance` | `numeric string` | 借券賣出當日餘額（股），公式：前日餘額 + 當日賣出 - 當日還券 + 當日調整。法規另有借券餘額上限（個別 10%、與融券合併 25%，詳第 13 節）。 | `TWT93U` 欄位「當日餘額」與官方 notes |
| `quota_sbl_next_limit` | `numeric string` | 次一營業日可借券賣出限額（股）。與「前 30 個營業日日平均成交量 30%」總量控管規則相關（詳第 13 節）。 | `TWT93U` 欄位「次一營業日可限額」 |
| `quota_note` | `string` | 信用交易備註符號（可同時多符號）；符號意義見第 11 節。 | `TWT93U` 欄位「備註」 |
| `inst_foreign_buy` | `numeric string` | 外陸資買進股數（不含外資自營商）。 | `T86` 欄位「外陸資買進股數(不含外資自營商)」 |
| `inst_foreign_sell` | `numeric string` | 外陸資賣出股數（不含外資自營商）。 | `T86` 欄位「外陸資賣出股數(不含外資自營商)」 |
| `inst_foreign_net` | `signed numeric string` | 外陸資買賣超股數（不含外資自營商）。 | `T86` 欄位「外陸資買賣超股數(不含外資自營商)」 |
| `inst_foreign_dealer_buy` | `numeric string` | 外資自營商買進股數。 | `T86` 欄位「外資自營商買進股數」 |
| `inst_foreign_dealer_sell` | `numeric string` | 外資自營商賣出股數。 | `T86` 欄位「外資自營商賣出股數」 |
| `inst_foreign_dealer_net` | `signed numeric string` | 外資自營商買賣超股數。 | `T86` 欄位「外資自營商買賣超股數」 |
| `inst_investment_trust_buy` | `numeric string` | 投信買進股數。 | `T86` 欄位「投信買進股數」 |
| `inst_investment_trust_sell` | `numeric string` | 投信賣出股數。 | `T86` 欄位「投信賣出股數」 |
| `inst_investment_trust_net` | `signed numeric string` | 投信買賣超股數。 | `T86` 欄位「投信買賣超股數」 |
| `inst_dealer_total_net` | `signed numeric string` | 自營商買賣超股數（合計，含自行買賣與避險）。 | `T86` 欄位「自營商買賣超股數」 |
| `inst_dealer_self_buy` | `numeric string` | 自營商（自行買賣）買進股數。 | `T86` 欄位「自營商買進股數(自行買賣)」 |
| `inst_dealer_self_sell` | `numeric string` | 自營商（自行買賣）賣出股數。 | `T86` 欄位「自營商賣出股數(自行買賣)」 |
| `inst_dealer_self_net` | `signed numeric string` | 自營商（自行買賣）買賣超股數。 | `T86` 欄位「自營商買賣超股數(自行買賣)」 |
| `inst_dealer_hedge_buy` | `numeric string` | 自營商（避險）買進股數。 | `T86` 欄位「自營商買進股數(避險)」 |
| `inst_dealer_hedge_sell` | `numeric string` | 自營商（避險）賣出股數。 | `T86` 欄位「自營商賣出股數(避險)」 |
| `inst_dealer_hedge_net` | `signed numeric string` | 自營商（避險）買賣超股數。 | `T86` 欄位「自營商買賣超股數(避險)」 |
| `inst_three_major_net` | `signed numeric string` | 三大法人買賣超股數（外資 + 投信 + 自營商）。 | `T86` 欄位「三大法人買賣超股數」 |
| `issued_shares` | `numeric string` | 計算周轉率使用的已發行股數（股）。優先用當日 `MI_QFIIS.發行股數`，無資料則回退 `t187ap03_L`。 | `MI_QFIIS` / `t187ap03_L` |
| `issued_shares_source` | `enum string` | `issued_shares` 的來源標記，值為 `MI_QFIIS` 或 `t187ap03_L`。 | 程式標記 |
| `turnover_ratio_pct` | `decimal string` | 當日周轉率（%）=`price_volume / issued_shares * 100`，輸出到小數點後 6 位。 | 程式計算 |
| `sbl_available_volume_latest` | `numeric string` | 可借券賣出股數快照（股）。注意：此欄是查詢當下的最新快照，不一定等於 `date` 當日值。 | `TWT96U` 欄位 `TWSEAvailableVolume` |

## 10) `all-in-one`（單股整合）

`all-in-one` 欄位結構與全市場整合大致相同，差異如下：

- 含 `price_note`（`STOCK_DAY` 註記）
- 含 `industry_code`（上市公司產業別代碼）
- 含次一交易日當沖狀態：`next_can_daytrade`、`next_daytrade_type`、`next_daytrade_suspension_flag`
- 融資融券欄位名稱同 `market-day-all`，使用 `margn_*` 前綴
- 當沖欄位包含 `daytrade_suspension_flag`
- 信用額度欄位使用 `quota_*` 前綴
- 含三大法人欄位：`inst_*`（`T86`）
- `sbl_available_volume_latest` 為該股票最新可借券賣出股數快照

## 11) 信用交易備註符號（`TWT93U`）

適用欄位：

- `credit-quota` 的 `note`
- `market-day-all` / `market-range-all` 的 `quota_note`
- `all-in-one` 的 `quota_note`

| 符號 | 意義 |
|---|---|
| `X` | 停券 |
| `Y` | 未取得信用交易資格 |
| `V` | 不得借券交易且無借券餘額停止借券賣出 |
| `%` | 信用額度分配 |
| `Z` | 借券賣出餘額已達總量控管標準或初次上市無賣出額度，暫停借券賣出 |
| `!` | 停止買賣（有些畫面會顯示為 `!─`） |

補充：

- 同一檔股票同一天可能同時出現多個符號，代表多個限制同時成立。
- 若欄位為空字串，表示該日無上述特殊註記。

## 還原欄位（`adj_*`）補充說明

- `market-range-all`：
  - 下載階段先寫入原始值（`adj_factor_back=1`，`adj_* = price_*`）
  - 完成後會自動掃描 `data/market_day_all_*.csv`，用 `TWT49U` 回補 `adj_*`
- `adjusted-price` / `all-in-one`：依你指定的 `start~end` 範圍計算 `adj_*`

因此 `adj_factor_back` 會和你持有的歷史範圍有關（尤其是是否已包含較新的除權息事件）。

## 12) 官方查證來源（2026-03-07 核對）

以下欄位說明是以官方回傳欄位名稱（`fields`）、分組（`groups`）、提示（`hints`）與註解（`notes`）核對：

- [`MI_INDEX` 每日收盤行情（全部，不含權證牛熊證）](https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?response=json&date=20260306&type=ALLBUT0999)
- [`TWT84U` 升降幅度與開盤競價基準](https://www.twse.com.tw/rwd/zh/variation/TWT84U?response=json&date=20260306&selectType=ALLBUT0999)
- [`MI_MARGN` 融資融券彙總](https://www.twse.com.tw/exchangeReport/MI_MARGN?response=json&date=20260306&selectType=ALL)
- [`TWTB4U` 當日沖銷交易標的及成交量值](https://www.twse.com.tw/exchangeReport/TWTB4U?response=json&date=20260306)
- [`TWT93U` 信用額度總量管制餘額表（含符號說明）](https://www.twse.com.tw/exchangeReport/TWT93U?response=json&date=20260306)
- [`T86` 三大法人買賣超日報](https://www.twse.com.tw/rwd/zh/fund/T86?date=20260306&selectType=ALLBUT0999&response=json)
- [`MI_QFIIS` 發行股數與外資持股資訊](https://www.twse.com.tw/fund/MI_QFIIS?response=json&date=20260306&selectType=ALLBUT0999)
- [`TWT96U` 上市可借券賣出股數（快照）](https://openapi.twse.com.tw/v1/SBL/TWT96U)
- [`t187ap03_L` 上市公司基本資料（含已發行股數）](https://openapi.twse.com.tw/v1/opendata/t187ap03_L)
- [`TWT49U` 除權息計算結果與公式](https://www.twse.com.tw/rwd/zh/exRight/TWT49U?response=json&startDate=20260101&endDate=20260306)
- [`STOCK_DAY` 個股日線欄位定義](https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date=20260301&stockNo=2330)

## 13) 借券賣出 / 融資融券法規與比例上限（對應欄位）

以下為和本 schema 欄位最直接相關、且會影響數值上限或可用額度的規則（2026-03-07 依官方公告核對）：

| 規則 | 限制值 | 對應欄位 | 依據（官方） |
|---|---|---|---|
| 融資比率上限 | 不得超過 60% | `margn_margin_*`（融資部位） | 金管會 2025-05-16 令（證券商辦理有價證券買賣融資融券業務操作辦法第 5 條）與證交所公告：[連結](https://twse-regulation.twse.com.tw/TW/law/DAT0202.aspx?FLCODE=FL007378&LCC=2&LCNOS=+7%2C+8%2C+11) |
| 融券保證金成數下限 | 不得低於 90% | `margn_short_*`、`quota_margin_short_*`（融券部位） | 同上（第 6 條）：[連結](https://twse-regulation.twse.com.tw/TW/law/DAT0202.aspx?FLCODE=FL007378&LCC=2&LCNOS=+7%2C+8%2C+11) |
| 融券+借券賣出「合併」總量控管 | 同一標的：融券賣出餘額 + 借券賣出餘額 > 流通在外股數 25% 時，停止新增賣出 | `quota_margin_short_balance`、`quota_sbl_balance`、`quota_note` | 證交所借券賣出總量控管規定（114.05.26）第 1 點第 1 款：[連結](https://twse-regulation.twse.com.tw/TW/law/DAT0202.aspx?FLCODE=FL007378&LCC=2&LCNOS=+7%2C+8%2C+11) |
| 借券賣出單獨餘額上限 | 借券賣出餘額 > 流通在外股數 10% 時，停止新增借券賣出 | `quota_sbl_balance`、`quota_note` | 同上第 1 點第 2 款：[連結](https://twse-regulation.twse.com.tw/TW/law/DAT0202.aspx?FLCODE=FL007378&LCC=2&LCNOS=+7%2C+8%2C+11) |
| 次一營業日可借券賣出限額（日量控管） | 每日盤中借券賣出委託量不得超過前 30 個營業日日平均成交量 30% | `quota_sbl_next_limit`、`quota_note` | 同上第 1 點第 3 款與 `TWT93U` notes：[連結](https://www.twse.com.tw/exchangeReport/TWT93U?response=json&date=20260306) |

補充：

- `quota_note` 的 `Z` 通常對應「已達總量控管標準或初次上市無賣出額度」。
- 法規可能調整，建議定期重抓最新公告並更新 schema 限制值。
