#!/usr/bin/env python3
import sys
from pathlib import Path
import chardet


def main(root_dir: Path):
    if not root_dir.exists():
        print(f"❌ 指定ディレクトリが存在しません: {root_dir}")
        sys.exit(1)

    # 出力先: root_dir/output_csv
    output_root = (root_dir / "output_csv").resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    print(f"出力先: {output_root}")

    # 対象となる CSV ファイルを再帰的に取得（output_csv 配下は除外）
    csv_files = []
    for p in root_dir.rglob("*.csv"):
        # output_csv 以下は除外
        if output_root in p.parents:
            continue
        if p.is_file():
            csv_files.append(p)

    if not csv_files:
        print("⚠️ CSV ファイルが見つかりませんでした。")
        return

    # 名前順にソートして連番を振る
    csv_files.sort(key=lambda x: x.name)

    total = len(csv_files)
    print(f"対象 CSV ファイル数: {total} 件")

    for idx, src in enumerate(csv_files, start=1):
        dst = output_root / f"{idx}.csv"

        try:
            # バイナリ読み込みして文字コード推定
            raw = src.read_bytes()
            detected = chardet.detect(raw) or {}
            enc = detected.get("encoding") or "cp932"

            print(f"[{idx}] {src.relative_to(root_dir)} "
                  f"(encoding={enc}) -> {dst.relative_to(root_dir)}")

            text = raw.decode(enc, errors="ignore")
            dst.write_text(text, encoding="utf-8", newline="")
        except Exception as e:
            print(f"⚠️ 変換失敗: {src} ({e})")

    print("========== サマリ ==========")
    print(f"変換・出力した CSV: {total} 件")
    if total != 125:
        print(f"※ 注意: 現在 {total} 件です。1〜125 を完全に揃えるには、元の CSV が 125 件必要です。")


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        root = Path(sys.argv[1]).expanduser().resolve()
    else:
        # 引数がなければカレントディレクトリ直下の csv/new を使う
        root = (Path.cwd() / "csv" / "new").resolve()
    main(root)