# Geospatial Challenge 2025: 不動産価格予測パイプライン

Snowflake + dbt + LightGBM を活用した、不動産価格予測コンペティション（SIGNATE）のソリューションです。
データの欠損補完から特徴量エンジニアリングまでをモダンデータスタックで実装しています。

## 🚀 成果
- **スコア**: MAE 19.8 (現在改善中、14点台を目標)
- **技術スタック**: Snowflake, dbt, Python (LightGBM)

## 🏗️ システム構成
Snowflake上の Raw Data を dbt で加工し、高解像度な Mart 層を構築。その後、Snowflake Notebook 上で ML モデルを構築しています。



### 1. Data Transformation (dbt)
- **Staging層**: 面積（unit_area）の欠損を `unit_area_max/min` から復元。
- **Mart層**: 
    - 築年数を YYYYMM 形式から「月単位の経過月数」へ高精度化。
    - 伏せ字（＊＊＊＊）駅名の座標ベースでの復元。
    - `FULL_ADDRESS` に基づくミクロな住所情報の抽出。

### 2. Machine Learning (Python/LightGBM)
- **OOF Target Encoding**: `BUILDING_ID` や `ADDR_GROUP` に対してリーク（情報漏洩）を防ぐ OOF 手法を実装。
- **交互作用特徴量**: `HOUSE_AREA * BUILDING_AGE` 等の非線形な関係を明示的に追加。
- **ハイパーパラメータ最適化**: 15分の実行制限を考慮した効率的な 4-Fold 交差検証。

## 📂 フォルダ構成
- `/models`: dbt の SQL モデル定義
- `/notebooks`: Snowflake Notebook で実行した学習用スクリプト

## ✍️ 開発記（Zenn）
このプロジェクトの試行錯誤については、Zennにて発信しています。
[Zenn: Snowflake + dbt で挑む不動産コンペ、20点の壁を「データの解像度」でこじ開けたい](https://zenn.dev/mfujita2023)
